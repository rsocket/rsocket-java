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
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.frame.ResumeFrameFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.internal.ClientSetup;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.lease.LeaseStats;
import io.rsocket.lease.Leases;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.lease.ResponderLeaseHandler;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.PluginRegistry;
import io.rsocket.plugins.Plugins;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.plugins.SocketAcceptorInterceptor;
import io.rsocket.resume.ExponentialBackoffResumeStrategy;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.resume.ResumableFramesStore;
import io.rsocket.resume.ResumeStrategy;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.EmptyPayload;
import io.rsocket.util.MultiSubscriberRSocket;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Default implementation of {@link RSocketFactory.ClientRSocketFactory} that can be instantiated
 * directly or through the shortcut {@link RSocketFactory#connect()}.
 */
public class DefaultClientRSocketFactory implements RSocketFactory.ClientRSocketFactory {
  private static final String CLIENT_TAG = "client";

  private static final BiConsumer<RSocket, Invalidatable> INVALIDATE_FUNCTION =
      (r, i) -> r.onClose().subscribe(null, null, i::invalidate);

  private SocketAcceptor acceptor = (setup, sendingSocket) -> Mono.just(new AbstractRSocket() {});

  private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
  private int mtu = 0;
  private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());

  private Payload setupPayload = EmptyPayload.INSTANCE;
  private PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;

  private Duration tickPeriod = Duration.ofSeconds(20);
  private Duration ackTimeout = Duration.ofSeconds(30);
  private int missedAcks = 3;

  private String metadataMimeType = "application/binary";
  private String dataMimeType = "application/binary";

  private boolean resumeEnabled;
  private boolean resumeCleanupStoreOnKeepAlive;
  private Supplier<ByteBuf> resumeTokenSupplier = ResumeFrameFlyweight::generateResumeToken;
  private Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory =
      token -> new InMemoryResumableFramesStore(CLIENT_TAG, 100_000);
  private Duration resumeSessionDuration = Duration.ofMinutes(2);
  private Duration resumeStreamTimeout = Duration.ofSeconds(10);
  private Supplier<ResumeStrategy> resumeStrategySupplier =
      () -> new ExponentialBackoffResumeStrategy(Duration.ofSeconds(1), Duration.ofSeconds(16), 2);

  private boolean multiSubscriberRequester = true;
  private boolean leaseEnabled;
  private Supplier<Leases<?>> leasesSupplier = Leases::new;
  private boolean reconnectEnabled;
  private Retry retrySpec;

  private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @Override
  public RSocketFactory.ClientRSocketFactory byteBufAllocator(ByteBufAllocator allocator) {
    Objects.requireNonNull(allocator);
    this.allocator = allocator;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory addConnectionPlugin(
      DuplexConnectionInterceptor interceptor) {
    plugins.addConnectionPlugin(interceptor);
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory addRequesterPlugin(RSocketInterceptor interceptor) {
    plugins.addRequesterPlugin(interceptor);
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory addResponderPlugin(RSocketInterceptor interceptor) {
    plugins.addResponderPlugin(interceptor);
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory addSocketAcceptorPlugin(
      SocketAcceptorInterceptor interceptor) {
    plugins.addSocketAcceptorPlugin(interceptor);
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory keepAlive(
      Duration tickPeriod, Duration ackTimeout, int missedAcks) {
    this.tickPeriod = tickPeriod;
    this.ackTimeout = ackTimeout;
    this.missedAcks = missedAcks;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory keepAliveTickPeriod(Duration tickPeriod) {
    this.tickPeriod = tickPeriod;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory keepAliveAckTimeout(Duration ackTimeout) {
    this.ackTimeout = ackTimeout;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory keepAliveMissedAcks(int missedAcks) {
    this.missedAcks = missedAcks;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory mimeType(
      String metadataMimeType, String dataMimeType) {
    this.dataMimeType = dataMimeType;
    this.metadataMimeType = metadataMimeType;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory dataMimeType(String dataMimeType) {
    this.dataMimeType = dataMimeType;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory metadataMimeType(String metadataMimeType) {
    this.metadataMimeType = metadataMimeType;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory lease(
      Supplier<Leases<? extends LeaseStats>> leasesSupplier) {
    this.leaseEnabled = true;
    this.leasesSupplier = Objects.requireNonNull(leasesSupplier);
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory lease() {
    this.leaseEnabled = true;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory singleSubscriberRequester() {
    this.multiSubscriberRequester = false;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory reconnect(Retry retrySpec) {
    this.retrySpec = Objects.requireNonNull(retrySpec);
    this.reconnectEnabled = true;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory resume() {
    this.resumeEnabled = true;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory resumeToken(Supplier<ByteBuf> resumeTokenSupplier) {
    this.resumeTokenSupplier = Objects.requireNonNull(resumeTokenSupplier);
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory resumeStore(
      Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory) {
    this.resumeStoreFactory = resumeStoreFactory;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory resumeSessionDuration(Duration sessionDuration) {
    this.resumeSessionDuration = Objects.requireNonNull(sessionDuration);
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory resumeStreamTimeout(Duration resumeStreamTimeout) {
    this.resumeStreamTimeout = Objects.requireNonNull(resumeStreamTimeout);
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory resumeStrategy(
      Supplier<ResumeStrategy> resumeStrategy) {
    this.resumeStrategySupplier = Objects.requireNonNull(resumeStrategy);
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory resumeCleanupOnKeepAlive() {
    resumeCleanupStoreOnKeepAlive = true;
    return this;
  }

  @Override
  public RSocketFactory.Start<RSocket> transport(Supplier<ClientTransport> transportClient) {
    return new StartClient(transportClient);
  }

  @Override
  public RSocketFactory.ClientTransportAcceptor acceptor(Function<RSocket, RSocket> acceptor) {
    return acceptor(() -> acceptor);
  }

  @Override
  public RSocketFactory.ClientTransportAcceptor acceptor(
      Supplier<Function<RSocket, RSocket>> acceptor) {
    return acceptor((setup, sendingSocket) -> Mono.just(acceptor.get().apply(sendingSocket)));
  }

  @Override
  public RSocketFactory.ClientTransportAcceptor acceptor(SocketAcceptor acceptor) {
    this.acceptor = acceptor;
    return StartClient::new;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory fragment(int mtu) {
    this.mtu = mtu;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
    this.errorConsumer = errorConsumer;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory setupPayload(Payload payload) {
    this.setupPayload = payload;
    return this;
  }

  @Override
  public RSocketFactory.ClientRSocketFactory frameDecoder(PayloadDecoder payloadDecoder) {
    this.payloadDecoder = payloadDecoder;
    return this;
  }

  private class StartClient implements RSocketFactory.Start<RSocket> {
    private final Supplier<ClientTransport> transportClient;

    StartClient(Supplier<ClientTransport> transportClient) {
      this.transportClient = transportClient;
    }

    @Override
    public Mono<RSocket> start() {
      return newConnection()
          .flatMap(
              connection -> {
                ClientSetup clientSetup = clientSetup(connection);
                ByteBuf resumeToken = clientSetup.resumeToken();
                KeepAliveHandler keepAliveHandler = clientSetup.keepAliveHandler();
                DuplexConnection wrappedConnection = clientSetup.connection();

                ClientServerInputMultiplexer multiplexer =
                    new ClientServerInputMultiplexer(wrappedConnection, plugins, true);

                boolean isLeaseEnabled = leaseEnabled;
                Leases<?> leases = leasesSupplier.get();
                RequesterLeaseHandler requesterLeaseHandler =
                    isLeaseEnabled
                        ? new RequesterLeaseHandler.Impl(CLIENT_TAG, leases.receiver())
                        : RequesterLeaseHandler.None;

                RSocket rSocketRequester =
                    new RSocketRequester(
                        allocator,
                        multiplexer.asClientConnection(),
                        payloadDecoder,
                        errorConsumer,
                        StreamIdSupplier.clientSupplier(),
                        keepAliveTickPeriod(),
                        keepAliveTimeout(),
                        keepAliveHandler,
                        requesterLeaseHandler);

                if (multiSubscriberRequester) {
                  rSocketRequester = new MultiSubscriberRSocket(rSocketRequester);
                }

                RSocket wrappedRSocketRequester = plugins.applyRequester(rSocketRequester);

                ByteBuf setupFrame =
                    SetupFrameFlyweight.encode(
                        allocator,
                        isLeaseEnabled,
                        keepAliveTickPeriod(),
                        keepAliveTimeout(),
                        resumeToken,
                        metadataMimeType,
                        dataMimeType,
                        setupPayload);

                ConnectionSetupPayload setup = new DefaultConnectionSetupPayload(setupFrame);

                return plugins
                    .applySocketAcceptorInterceptor(acceptor)
                    .accept(setup, wrappedRSocketRequester)
                    .flatMap(
                        rSocketHandler -> {
                          RSocket wrappedRSocketHandler = plugins.applyResponder(rSocketHandler);

                          ResponderLeaseHandler responderLeaseHandler =
                              isLeaseEnabled
                                  ? new ResponderLeaseHandler.Impl<>(
                                      CLIENT_TAG,
                                      allocator,
                                      leases.sender(),
                                      errorConsumer,
                                      leases.stats())
                                  : ResponderLeaseHandler.None;

                          RSocket rSocketResponder =
                              new RSocketResponder(
                                  allocator,
                                  multiplexer.asServerConnection(),
                                  wrappedRSocketHandler,
                                  payloadDecoder,
                                  errorConsumer,
                                  responderLeaseHandler);

                          return wrappedConnection
                              .sendOne(setupFrame)
                              .thenReturn(wrappedRSocketRequester);
                        });
              })
          .as(
              source -> {
                if (reconnectEnabled) {
                  return new ReconnectMono<>(
                      source.retryWhen(retrySpec), Disposable::dispose, INVALIDATE_FUNCTION);
                } else {
                  return source;
                }
              });
    }

    private int keepAliveTickPeriod() {
      return (int) tickPeriod.toMillis();
    }

    private int keepAliveTimeout() {
      return (int) (ackTimeout.toMillis() + tickPeriod.toMillis() * missedAcks);
    }

    private ClientSetup clientSetup(DuplexConnection startConnection) {
      if (resumeEnabled) {
        ByteBuf resumeToken = resumeTokenSupplier.get();
        return new ClientSetup.ResumableClientSetup(
            allocator,
            startConnection,
            newConnection(),
            resumeToken,
            resumeStoreFactory.apply(resumeToken),
            resumeSessionDuration,
            resumeStreamTimeout,
            resumeStrategySupplier,
            resumeCleanupStoreOnKeepAlive);
      } else {
        return new ClientSetup.DefaultClientSetup(startConnection);
      }
    }

    private Mono<DuplexConnection> newConnection() {
      return Mono.fromSupplier(transportClient).flatMap(t -> t.connect(mtu));
    }
  }
}
