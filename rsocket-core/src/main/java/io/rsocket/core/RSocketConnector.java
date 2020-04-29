/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.lease.LeaseStats;
import io.rsocket.lease.Leases;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.lease.ResponderLeaseHandler;
import io.rsocket.plugins.InitializingInterceptorRegistry;
import io.rsocket.plugins.InterceptorRegistry;
import io.rsocket.resume.ClientRSocketSession;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class RSocketConnector {
  private static final String CLIENT_TAG = "client";
  private static final int MIN_MTU_SIZE = 64;

  private static final BiConsumer<RSocket, Invalidatable> INVALIDATE_FUNCTION =
      (r, i) -> r.onClose().subscribe(null, __ -> i.invalidate(), i::invalidate);

  private Payload setupPayload = EmptyPayload.INSTANCE;
  private String metadataMimeType = "application/binary";
  private String dataMimeType = "application/binary";

  private SocketAcceptor acceptor = SocketAcceptor.with(new RSocket() {});
  private InitializingInterceptorRegistry interceptors = new InitializingInterceptorRegistry();

  private Duration keepAliveInterval = Duration.ofSeconds(20);
  private Duration keepAliveMaxLifeTime = Duration.ofSeconds(90);

  private Retry retrySpec;
  private Resume resume;
  private Supplier<Leases<?>> leasesSupplier;

  private int mtu = 0;
  private PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;

  private Consumer<Throwable> errorConsumer = ex -> {};

  private RSocketConnector() {}

  public static RSocketConnector create() {
    return new RSocketConnector();
  }

  public static Mono<RSocket> connectWith(ClientTransport transport) {
    return RSocketConnector.create().connect(() -> transport);
  }

  public RSocketConnector setupPayload(Payload payload) {
    this.setupPayload = payload;
    return this;
  }

  public RSocketConnector dataMimeType(String dataMimeType) {
    this.dataMimeType = dataMimeType;
    return this;
  }

  public RSocketConnector metadataMimeType(String metadataMimeType) {
    this.metadataMimeType = metadataMimeType;
    return this;
  }

  public RSocketConnector interceptors(Consumer<InterceptorRegistry> consumer) {
    consumer.accept(this.interceptors);
    return this;
  }

  public RSocketConnector acceptor(SocketAcceptor acceptor) {
    this.acceptor = acceptor;
    return this;
  }

  /**
   * Set the time {@code interval} between KEEPALIVE frames sent by this client, and the {@code
   * maxLifeTime} that this client will allow between KEEPALIVE frames from the server before
   * assuming it is dead.
   *
   * <p>Note that reasonable values for the time interval may vary significantly. For
   * server-to-server connections the spec suggests 500ms, while for for mobile-to-server
   * connections it suggests 30+ seconds. In addition {@code maxLifeTime} should allow plenty of
   * room for multiple missed ticks from the server.
   *
   * <p>By default {@code interval} is set to 20 seconds and {@code maxLifeTime} to 90 seconds.
   *
   * @param interval the time between KEEPALIVE frames sent, must be greater than 0.
   * @param maxLifeTime the max time between KEEPALIVE frames received, must be greater than 0.
   */
  public RSocketConnector keepAlive(Duration interval, Duration maxLifeTime) {
    if (!interval.negated().isNegative()) {
      throw new IllegalArgumentException("`interval` for keepAlive must be > 0");
    }
    if (!maxLifeTime.negated().isNegative()) {
      throw new IllegalArgumentException("`maxLifeTime` for keepAlive must be > 0");
    }
    this.keepAliveInterval = interval;
    this.keepAliveMaxLifeTime = maxLifeTime;
    return this;
  }

  /**
   * Enables a reconnectable, shared instance of {@code Mono<RSocket>} so every subscriber will
   * observe the same RSocket instance up on connection establishment. <br>
   * For example:
   *
   * <pre>{@code
   * Mono<RSocket> sharedRSocketMono =
   *   RSocketConnector.create()
   *           .reconnect(Retry.fixedDelay(3, Duration.ofSeconds(1)))
   *           .connect(transport);
   *
   *  RSocket r1 = sharedRSocketMono.block();
   *  RSocket r2 = sharedRSocketMono.block();
   *
   *  assert r1 == r2;
   *
   * }</pre>
   *
   * Apart of the shared behavior, if the connection is lost, the same {@code Mono<RSocket>}
   * instance will transparently re-establish the connection for subsequent subscribers.<br>
   * For example:
   *
   * <pre>{@code
   * Mono<RSocket> sharedRSocketMono =
   *   RSocketConnector.create()
   *           .reconnect(Retry.fixedDelay(3, Duration.ofSeconds(1)))
   *           .connect(transport);
   *
   *  RSocket r1 = sharedRSocketMono.block();
   *  RSocket r2 = sharedRSocketMono.block();
   *
   *  assert r1 == r2;
   *
   *  r1.dispose()
   *
   *  assert r2.isDisposed()
   *
   *  RSocket r3 = sharedRSocketMono.block();
   *  RSocket r4 = sharedRSocketMono.block();
   *
   *  assert r1 != r3;
   *  assert r4 == r3;
   *
   * }</pre>
   *
   * <b>Note,</b> having reconnect() enabled does not eliminate the need to accompany each
   * individual request with the corresponding retry logic. <br>
   * For example:
   *
   * <pre>{@code
   * Mono<RSocket> sharedRSocketMono =
   *   RSocketConnector.create()
   *           .reconnect(Retry.fixedDelay(3, Duration.ofSeconds(1)))
   *           .connect(transport);
   *
   *  sharedRSocket.flatMap(rSocket -> rSocket.requestResponse(...))
   *               .retryWhen(ownRetry)
   *               .subscribe()
   *
   * }</pre>
   *
   * @param retrySpec a retry factory applied for {@link Mono#retryWhen(Retry)}
   * @return a shared instance of {@code Mono<RSocket>}.
   */
  public RSocketConnector reconnect(Retry retrySpec) {
    this.retrySpec = Objects.requireNonNull(retrySpec);
    return this;
  }

  public RSocketConnector resume(Resume resume) {
    this.resume = resume;
    return this;
  }

  public RSocketConnector lease(Supplier<Leases<? extends LeaseStats>> supplier) {
    this.leasesSupplier = supplier;
    return this;
  }

  public RSocketConnector fragment(int mtu) {
    if (mtu > 0 && mtu < MIN_MTU_SIZE || mtu < 0) {
      String msg =
          String.format("smallest allowed mtu size is %d bytes, provided: %d", MIN_MTU_SIZE, mtu);
      throw new IllegalArgumentException(msg);
    }
    this.mtu = mtu;
    return this;
  }

  public RSocketConnector payloadDecoder(PayloadDecoder payloadDecoder) {
    Objects.requireNonNull(payloadDecoder);
    this.payloadDecoder = payloadDecoder;
    return this;
  }

  /**
   * @deprecated this is deprecated with no replacement and will be removed after {@link
   *     io.rsocket.RSocketFactory} is removed.
   */
  @Deprecated
  public RSocketConnector errorConsumer(Consumer<Throwable> errorConsumer) {
    Objects.requireNonNull(errorConsumer);
    this.errorConsumer = errorConsumer;
    return this;
  }

  public Mono<RSocket> connect(ClientTransport transport) {
    return connect(() -> transport);
  }

  public Mono<RSocket> connect(Supplier<ClientTransport> transportSupplier) {
    Mono<DuplexConnection> connectionMono =
        Mono.fromSupplier(transportSupplier).flatMap(t -> t.connect(mtu));
    return connectionMono
        .flatMap(
            connection -> {
              ByteBuf resumeToken;
              KeepAliveHandler keepAliveHandler;
              DuplexConnection wrappedConnection;

              if (resume != null) {
                resumeToken = resume.getTokenSupplier().get();
                ClientRSocketSession session =
                    new ClientRSocketSession(
                            connection,
                            resume.getSessionDuration(),
                            resume.getRetry(),
                            resume.getStoreFactory(CLIENT_TAG).apply(resumeToken),
                            resume.getStreamTimeout(),
                            resume.isCleanupStoreOnKeepAlive())
                        .continueWith(connectionMono)
                        .resumeToken(resumeToken);
                keepAliveHandler =
                    new KeepAliveHandler.ResumableKeepAliveHandler(session.resumableConnection());
                wrappedConnection = session.resumableConnection();
              } else {
                resumeToken = Unpooled.EMPTY_BUFFER;
                keepAliveHandler = new KeepAliveHandler.DefaultKeepAliveHandler(connection);
                wrappedConnection = connection;
              }

              ClientServerInputMultiplexer multiplexer =
                  new ClientServerInputMultiplexer(wrappedConnection, interceptors, true);

              boolean leaseEnabled = leasesSupplier != null;
              Leases<?> leases = leaseEnabled ? leasesSupplier.get() : null;
              RequesterLeaseHandler requesterLeaseHandler =
                  leaseEnabled
                      ? new RequesterLeaseHandler.Impl(CLIENT_TAG, leases.receiver())
                      : RequesterLeaseHandler.None;

              RSocket rSocketRequester =
                  new RSocketRequester(
                      multiplexer.asClientConnection(),
                      payloadDecoder,
                      errorConsumer,
                      StreamIdSupplier.clientSupplier(),
                      mtu,
                      (int) keepAliveInterval.toMillis(),
                      (int) keepAliveMaxLifeTime.toMillis(),
                      keepAliveHandler,
                      requesterLeaseHandler);

              RSocket wrappedRSocketRequester = interceptors.initRequester(rSocketRequester);

              ByteBuf setupFrame =
                  SetupFrameFlyweight.encode(
                      wrappedConnection.alloc(),
                      leaseEnabled,
                      (int) keepAliveInterval.toMillis(),
                      (int) keepAliveMaxLifeTime.toMillis(),
                      resumeToken,
                      metadataMimeType,
                      dataMimeType,
                      setupPayload);

              ConnectionSetupPayload setup = new DefaultConnectionSetupPayload(setupFrame);

              return interceptors
                  .initSocketAcceptor(acceptor)
                  .accept(setup, wrappedRSocketRequester)
                  .flatMap(
                      rSocketHandler -> {
                        RSocket wrappedRSocketHandler = interceptors.initResponder(rSocketHandler);

                        ResponderLeaseHandler responderLeaseHandler =
                            leaseEnabled
                                ? new ResponderLeaseHandler.Impl<>(
                                    CLIENT_TAG,
                                    wrappedConnection.alloc(),
                                    leases.sender(),
                                    errorConsumer,
                                    leases.stats())
                                : ResponderLeaseHandler.None;

                        RSocket rSocketResponder =
                            new RSocketResponder(
                                multiplexer.asServerConnection(),
                                wrappedRSocketHandler,
                                payloadDecoder,
                                errorConsumer,
                                responderLeaseHandler,
                                mtu);

                        return wrappedConnection
                            .sendOne(setupFrame)
                            .thenReturn(wrappedRSocketRequester);
                      });
            })
        .as(
            source -> {
              if (retrySpec != null) {
                return new ReconnectMono<>(
                    source.retryWhen(retrySpec), Disposable::dispose, INVALIDATE_FUNCTION);
              } else {
                return source;
              }
            });
  }
}
