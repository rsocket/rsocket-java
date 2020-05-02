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
import io.rsocket.Closeable;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.exceptions.InvalidSetupException;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.lease.Leases;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.lease.ResponderLeaseHandler;
import io.rsocket.plugins.InitializingInterceptorRegistry;
import io.rsocket.plugins.InterceptorRegistry;
import io.rsocket.resume.SessionManager;
import io.rsocket.transport.ServerTransport;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

public final class RSocketServer {
  private static final String SERVER_TAG = "server";
  private static final int MIN_MTU_SIZE = 64;

  private SocketAcceptor acceptor = SocketAcceptor.with(new RSocket() {});
  private InitializingInterceptorRegistry interceptors = new InitializingInterceptorRegistry();
  private int mtu = 0;

  private Resume resume;
  private Supplier<Leases<?>> leasesSupplier = null;

  private Consumer<Throwable> errorConsumer = ex -> {};
  private PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;

  private RSocketServer() {}

  public static RSocketServer create() {
    return new RSocketServer();
  }

  public static RSocketServer create(SocketAcceptor acceptor) {
    return RSocketServer.create().acceptor(acceptor);
  }

  public RSocketServer acceptor(SocketAcceptor acceptor) {
    Objects.requireNonNull(acceptor);
    this.acceptor = acceptor;
    return this;
  }

  public RSocketServer interceptors(Consumer<InterceptorRegistry> consumer) {
    consumer.accept(this.interceptors);
    return this;
  }

  public RSocketServer fragment(int mtu) {
    if (mtu > 0 && mtu < MIN_MTU_SIZE || mtu < 0) {
      String msg =
          String.format("smallest allowed mtu size is %d bytes, provided: %d", MIN_MTU_SIZE, mtu);
      throw new IllegalArgumentException(msg);
    }
    this.mtu = mtu;
    return this;
  }

  public RSocketServer resume(Resume resume) {
    this.resume = resume;
    return this;
  }

  public RSocketServer lease(Supplier<Leases<?>> supplier) {
    this.leasesSupplier = supplier;
    return this;
  }

  public RSocketServer payloadDecoder(PayloadDecoder payloadDecoder) {
    Objects.requireNonNull(payloadDecoder);
    this.payloadDecoder = payloadDecoder;
    return this;
  }

  /**
   * @deprecated this is deprecated with no replacement and will be removed after {@link
   *     io.rsocket.RSocketFactory} is removed.
   */
  @Deprecated
  public RSocketServer errorConsumer(Consumer<Throwable> errorConsumer) {
    this.errorConsumer = errorConsumer;
    return this;
  }

  public ServerTransport.ConnectionAcceptor asConnectionAcceptor() {
    return new ServerTransport.ConnectionAcceptor() {
      private final ServerSetup serverSetup = serverSetup();

      @Override
      public Mono<Void> apply(DuplexConnection connection) {
        return acceptor(serverSetup, connection);
      }
    };
  }

  public <T extends Closeable> Mono<T> bind(ServerTransport<T> transport) {
    return Mono.defer(
        new Supplier<Mono<T>>() {
          ServerSetup serverSetup = serverSetup();

          @Override
          public Mono<T> get() {
            return transport
                .start(duplexConnection -> acceptor(serverSetup, duplexConnection), mtu)
                .doOnNext(c -> c.onClose().doFinally(v -> serverSetup.dispose()).subscribe());
          }
        });
  }

  private Mono<Void> acceptor(ServerSetup serverSetup, DuplexConnection connection) {
    ClientServerInputMultiplexer multiplexer =
        new ClientServerInputMultiplexer(connection, interceptors, false);

    return multiplexer
        .asSetupConnection()
        .receive()
        .next()
        .flatMap(startFrame -> accept(serverSetup, startFrame, multiplexer));
  }

  private Mono<Void> acceptResume(
      ServerSetup serverSetup, ByteBuf resumeFrame, ClientServerInputMultiplexer multiplexer) {
    return serverSetup.acceptRSocketResume(resumeFrame, multiplexer);
  }

  private Mono<Void> accept(
      ServerSetup serverSetup, ByteBuf startFrame, ClientServerInputMultiplexer multiplexer) {
    switch (FrameHeaderFlyweight.frameType(startFrame)) {
      case SETUP:
        return acceptSetup(serverSetup, startFrame, multiplexer);
      case RESUME:
        return acceptResume(serverSetup, startFrame, multiplexer);
      default:
        return serverSetup
            .sendError(
                multiplexer,
                new InvalidSetupException(
                    "invalid setup frame: " + FrameHeaderFlyweight.frameType(startFrame)))
            .doFinally(
                signalType -> {
                  startFrame.release();
                  multiplexer.dispose();
                });
    }
  }

  private Mono<Void> acceptSetup(
      ServerSetup serverSetup, ByteBuf setupFrame, ClientServerInputMultiplexer multiplexer) {

    if (!SetupFrameFlyweight.isSupportedVersion(setupFrame)) {
      return serverSetup
          .sendError(
              multiplexer,
              new InvalidSetupException(
                  "Unsupported version: " + SetupFrameFlyweight.humanReadableVersion(setupFrame)))
          .doFinally(
              signalType -> {
                setupFrame.release();
                multiplexer.dispose();
              });
    }

    boolean leaseEnabled = leasesSupplier != null;
    if (SetupFrameFlyweight.honorLease(setupFrame) && !leaseEnabled) {
      return serverSetup
          .sendError(multiplexer, new InvalidSetupException("lease is not supported"))
          .doFinally(
              signalType -> {
                setupFrame.release();
                multiplexer.dispose();
              });
    }

    return serverSetup.acceptRSocketSetup(
        setupFrame,
        multiplexer,
        (keepAliveHandler, wrappedMultiplexer) -> {
          ConnectionSetupPayload setupPayload = new DefaultConnectionSetupPayload(setupFrame);

          Leases<?> leases = leaseEnabled ? leasesSupplier.get() : null;
          RequesterLeaseHandler requesterLeaseHandler =
              leaseEnabled
                  ? new RequesterLeaseHandler.Impl(SERVER_TAG, leases.receiver())
                  : RequesterLeaseHandler.None;

          RSocket rSocketRequester =
              new RSocketRequester(
                  wrappedMultiplexer.asServerConnection(),
                  resume != null,
                  payloadDecoder,
                  errorConsumer,
                  StreamIdSupplier.serverSupplier(),
                  mtu,
                  setupPayload.keepAliveInterval(),
                  setupPayload.keepAliveMaxLifetime(),
                  keepAliveHandler,
                  requesterLeaseHandler);

          RSocket wrappedRSocketRequester = interceptors.initRequester(rSocketRequester);

          return interceptors
              .initSocketAcceptor(acceptor)
              .accept(setupPayload, wrappedRSocketRequester)
              .onErrorResume(
                  err ->
                      serverSetup
                          .sendError(multiplexer, rejectedSetupError(err))
                          .then(Mono.error(err)))
              .doOnNext(
                  rSocketHandler -> {
                    RSocket wrappedRSocketHandler = interceptors.initResponder(rSocketHandler);
                    DuplexConnection connection = wrappedMultiplexer.asClientConnection();

                    ResponderLeaseHandler responderLeaseHandler =
                        leaseEnabled
                            ? new ResponderLeaseHandler.Impl<>(
                                SERVER_TAG,
                                connection.alloc(),
                                leases.sender(),
                                errorConsumer,
                                leases.stats())
                            : ResponderLeaseHandler.None;

                    RSocket rSocketResponder =
                        new RSocketResponder(
                            connection,
                            wrappedRSocketHandler,
                            payloadDecoder,
                            errorConsumer,
                            responderLeaseHandler,
                            mtu);
                  })
              .doFinally(signalType -> setupPayload.release())
              .then();
        });
  }

  private ServerSetup serverSetup() {
    return resume != null ? createSetup() : new ServerSetup.DefaultServerSetup();
  }

  ServerSetup createSetup() {
    return new ServerSetup.ResumableServerSetup(
        new SessionManager(),
        resume.getSessionDuration(),
        resume.getStreamTimeout(),
        resume.getStoreFactory(SERVER_TAG),
        resume.isCleanupStoreOnKeepAlive());
  }

  private Exception rejectedSetupError(Throwable err) {
    String msg = err.getMessage();
    return new RejectedSetupException(msg == null ? "rejected by server acceptor" : msg);
  }
}
