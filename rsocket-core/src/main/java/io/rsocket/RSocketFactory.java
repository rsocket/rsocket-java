/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket;

import static io.rsocket.internal.ClientSetup.DefaultClientSetup;
import static io.rsocket.internal.ClientSetup.ResumableClientSetup;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.exceptions.InvalidSetupException;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.ResumeFrameFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.internal.ClientSetup;
import io.rsocket.internal.ServerSetup;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.lease.LeaseStats;
import io.rsocket.lease.Leases;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.lease.ResponderLeaseHandler;
import io.rsocket.plugins.*;
import io.rsocket.resume.*;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.ConnectionUtils;
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

/** Factory for creating RSocket clients and servers. */
public class RSocketFactory {
  /**
   * Creates a factory that establishes client connections to other RSockets.
   *
   * @return a client factory
   */
  public static ClientRSocketFactory connect() {
    return new ClientRSocketFactory();
  }

  /**
   * Creates a factory that receives server connections from client RSockets.
   *
   * @return a server factory.
   */
  public static ServerRSocketFactory receive() {
    return new ServerRSocketFactory();
  }

  public interface Start<T extends Closeable> {
    Mono<T> start();
  }

  public interface ClientTransportAcceptor {
    Start<RSocket> transport(Supplier<ClientTransport> transport);

    default Start<RSocket> transport(ClientTransport transport) {
      return transport(() -> transport);
    }
  }

  public interface ServerTransportAcceptor {

    ServerTransport.ConnectionAcceptor toConnectionAcceptor();

    <T extends Closeable> Start<T> transport(Supplier<ServerTransport<T>> transport);

    default <T extends Closeable> Start<T> transport(ServerTransport<T> transport) {
      return transport(() -> transport);
    }
  }

  public static class ClientRSocketFactory implements ClientTransportAcceptor {
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
        () ->
            new ExponentialBackoffResumeStrategy(Duration.ofSeconds(1), Duration.ofSeconds(16), 2);

    private boolean multiSubscriberRequester = true;
    private boolean leaseEnabled;
    private Supplier<Leases<?>> leasesSupplier = Leases::new;
    private boolean reconnectEnabled;
    private Retry retrySpec;

    private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    public ClientRSocketFactory byteBufAllocator(ByteBufAllocator allocator) {
      Objects.requireNonNull(allocator);
      this.allocator = allocator;
      return this;
    }

    public ClientRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      plugins.addConnectionPlugin(interceptor);
      return this;
    }
    /** Deprecated. Use {@link #addRequesterPlugin(RSocketInterceptor)} instead */
    @Deprecated
    public ClientRSocketFactory addClientPlugin(RSocketInterceptor interceptor) {
      return addRequesterPlugin(interceptor);
    }

    public ClientRSocketFactory addRequesterPlugin(RSocketInterceptor interceptor) {
      plugins.addRequesterPlugin(interceptor);
      return this;
    }

    /** Deprecated. Use {@link #addResponderPlugin(RSocketInterceptor)} instead */
    @Deprecated
    public ClientRSocketFactory addServerPlugin(RSocketInterceptor interceptor) {
      return addResponderPlugin(interceptor);
    }

    public ClientRSocketFactory addResponderPlugin(RSocketInterceptor interceptor) {
      plugins.addResponderPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory addSocketAcceptorPlugin(SocketAcceptorInterceptor interceptor) {
      plugins.addSocketAcceptorPlugin(interceptor);
      return this;
    }

    /**
     * Deprecated as Keep-Alive is not optional according to spec
     *
     * @return this ClientRSocketFactory
     */
    @Deprecated
    public ClientRSocketFactory keepAlive() {
      return this;
    }

    public ClientRSocketFactory keepAlive(
        Duration tickPeriod, Duration ackTimeout, int missedAcks) {
      this.tickPeriod = tickPeriod;
      this.ackTimeout = ackTimeout;
      this.missedAcks = missedAcks;
      return this;
    }

    public ClientRSocketFactory keepAliveTickPeriod(Duration tickPeriod) {
      this.tickPeriod = tickPeriod;
      return this;
    }

    public ClientRSocketFactory keepAliveAckTimeout(Duration ackTimeout) {
      this.ackTimeout = ackTimeout;
      return this;
    }

    public ClientRSocketFactory keepAliveMissedAcks(int missedAcks) {
      this.missedAcks = missedAcks;
      return this;
    }

    public ClientRSocketFactory mimeType(String metadataMimeType, String dataMimeType) {
      this.dataMimeType = dataMimeType;
      this.metadataMimeType = metadataMimeType;
      return this;
    }

    public ClientRSocketFactory dataMimeType(String dataMimeType) {
      this.dataMimeType = dataMimeType;
      return this;
    }

    public ClientRSocketFactory metadataMimeType(String metadataMimeType) {
      this.metadataMimeType = metadataMimeType;
      return this;
    }

    public ClientRSocketFactory lease(Supplier<Leases<? extends LeaseStats>> leasesSupplier) {
      this.leaseEnabled = true;
      this.leasesSupplier = Objects.requireNonNull(leasesSupplier);
      return this;
    }

    public ClientRSocketFactory lease() {
      this.leaseEnabled = true;
      return this;
    }

    public ClientRSocketFactory singleSubscriberRequester() {
      this.multiSubscriberRequester = false;
      return this;
    }

    /**
     * Enables a reconnectable, shared instance of {@code Mono<RSocket>} so every subscriber will
     * observe the same RSocket instance up on connection establishment. <br>
     * For example:
     *
     * <pre>{@code
     * Mono<RSocket> sharedRSocketMono =
     *   RSocketFactory
     *                .connect()
     *                .singleSubscriberRequester()
     *                .reconnect()
     *                .transport(transport)
     *                .start();
     *
     *  RSocket r1 = sharedRSocketMono.block();
     *  RSocket r2 = sharedRSocketMono.block();
     *
     *  assert r1 == r2;
     *
     * }</pre>
     *
     * Apart of the shared behavior, once the previous connection has been expired (e.g. connection
     * has been disposed), the same instance of {@code Mono<RSocket>} reestablish connection without
     * any extra effort. <br>
     * For example:
     *
     * <pre>{@code
     * Mono<RSocket> sharedRSocketMono =
     *   RSocketFactory
     *                .connect()
     *                .singleSubscriberRequester()
     *                .reconnect()
     *                .transport(transport)
     *                .start();
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
     *
     *  assert r1 != r3;
     *  assert r4 == r3;
     *
     * }</pre>
     *
     * @return a shared instance of {@code Mono<RSocket>}.
     */
    public ClientRSocketFactory reconnect() {
      this.reconnectEnabled = true;
      return this;
    }

    /**
     * Enables a reconnectable, shared instance of {@code Mono<RSocket>} so every subscriber will
     * observe the same RSocket instance up on connection establishment. <br>
     * For example:
     *
     * <pre>{@code
     * Mono<RSocket> sharedRSocketMono =
     *   RSocketFactory
     *                .connect()
     *                .singleSubscriberRequester()
     *                .reconnect(fluxOfThrowables -> flux.concatMap(t -> Mono.delay(Duration.ofSeconds(1)).take(5))
     *                .transport(transport)
     *                .start();
     *
     *  RSocket r1 = sharedRSocketMono.block();
     *  RSocket r2 = sharedRSocketMono.block();
     *
     *  assert r1 == r2;
     *
     * }</pre>
     *
     * Apart of the shared behavior, once the previous connection has been expired (e.g. connection
     * has been disposed), the same instance of {@code Mono<RSocket>} reestablish connection without
     * any extra effort. <br>
     * For example:
     *
     * <pre>{@code
     * Mono<RSocket> sharedRSocketMono =
     *   RSocketFactory
     *                .connect()
     *                .singleSubscriberRequester()
     *                .reconnect(fluxOfThrowables -> flux.concatMap(t -> Mono.delay(Duration.ofSeconds(1)).take(5))
     *                .transport(transport)
     *                .start();
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
     *
     *  assert r1 != r3;
     *  assert r4 == r3;
     *
     * }</pre>
     *
     * @param whenFactory a retry factory applied for {@link Mono.retryWhen(whenFactory)}
     * @return a shared instance of {@code Mono<RSocket>}.
     */
    public ClientRSocketFactory reconnect(Retry retrySpec) {
      this.retrySpec = Objects.requireNonNull(retrySpec);
      this.reconnectEnabled = true;
      return this;
    }

    public ClientRSocketFactory resume() {
      this.resumeEnabled = true;
      return this;
    }

    public ClientRSocketFactory resumeToken(Supplier<ByteBuf> resumeTokenSupplier) {
      this.resumeTokenSupplier = Objects.requireNonNull(resumeTokenSupplier);
      return this;
    }

    public ClientRSocketFactory resumeStore(
        Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory) {
      this.resumeStoreFactory = resumeStoreFactory;
      return this;
    }

    public ClientRSocketFactory resumeSessionDuration(Duration sessionDuration) {
      this.resumeSessionDuration = Objects.requireNonNull(sessionDuration);
      return this;
    }

    public ClientRSocketFactory resumeStreamTimeout(Duration resumeStreamTimeout) {
      this.resumeStreamTimeout = Objects.requireNonNull(resumeStreamTimeout);
      return this;
    }

    public ClientRSocketFactory resumeStrategy(Supplier<ResumeStrategy> resumeStrategy) {
      this.resumeStrategySupplier = Objects.requireNonNull(resumeStrategy);
      return this;
    }

    public ClientRSocketFactory resumeCleanupOnKeepAlive() {
      resumeCleanupStoreOnKeepAlive = true;
      return this;
    }

    @Override
    public Start<RSocket> transport(Supplier<ClientTransport> transportClient) {
      return new StartClient(transportClient);
    }

    public ClientTransportAcceptor acceptor(Function<RSocket, RSocket> acceptor) {
      return acceptor(() -> acceptor);
    }

    public ClientTransportAcceptor acceptor(Supplier<Function<RSocket, RSocket>> acceptor) {
      return acceptor((setup, sendingSocket) -> Mono.just(acceptor.get().apply(sendingSocket)));
    }

    public ClientTransportAcceptor acceptor(SocketAcceptor acceptor) {
      this.acceptor = acceptor;
      return StartClient::new;
    }

    public ClientRSocketFactory fragment(int mtu) {
      this.mtu = mtu;
      return this;
    }

    public ClientRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
      this.errorConsumer = errorConsumer;
      return this;
    }

    public ClientRSocketFactory setupPayload(Payload payload) {
      this.setupPayload = payload;
      return this;
    }

    public ClientRSocketFactory frameDecoder(PayloadDecoder payloadDecoder) {
      this.payloadDecoder = payloadDecoder;
      return this;
    }

    private class StartClient implements Start<RSocket> {
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

                  ConnectionSetupPayload setup = ConnectionSetupPayload.create(setupFrame);

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
                        retrySpec == null ? source : source.retryWhen(retrySpec),
                        Disposable::dispose,
                        INVALIDATE_FUNCTION);
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
          return new ResumableClientSetup(
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
          return new DefaultClientSetup(startConnection);
        }
      }

      private Mono<DuplexConnection> newConnection() {
        return Mono.fromSupplier(transportClient).flatMap(t -> t.connect(mtu));
      }
    }
  }

  public static class ServerRSocketFactory {
    private static final String SERVER_TAG = "server";

    private SocketAcceptor acceptor;
    private PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;
    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private int mtu = 0;
    private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());

    private boolean resumeSupported;
    private Duration resumeSessionDuration = Duration.ofSeconds(120);
    private Duration resumeStreamTimeout = Duration.ofSeconds(10);
    private Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory =
        token -> new InMemoryResumableFramesStore(SERVER_TAG, 100_000);

    private boolean multiSubscriberRequester = true;
    private boolean leaseEnabled;
    private Supplier<Leases<?>> leasesSupplier = Leases::new;

    private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private boolean resumeCleanupStoreOnKeepAlive;

    private ServerRSocketFactory() {}

    public ServerRSocketFactory byteBufAllocator(ByteBufAllocator allocator) {
      Objects.requireNonNull(allocator);
      this.allocator = allocator;
      return this;
    }

    public ServerRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      plugins.addConnectionPlugin(interceptor);
      return this;
    }
    /** Deprecated. Use {@link #addRequesterPlugin(RSocketInterceptor)} instead */
    @Deprecated
    public ServerRSocketFactory addClientPlugin(RSocketInterceptor interceptor) {
      return addRequesterPlugin(interceptor);
    }

    public ServerRSocketFactory addRequesterPlugin(RSocketInterceptor interceptor) {
      plugins.addRequesterPlugin(interceptor);
      return this;
    }

    /** Deprecated. Use {@link #addResponderPlugin(RSocketInterceptor)} instead */
    @Deprecated
    public ServerRSocketFactory addServerPlugin(RSocketInterceptor interceptor) {
      return addResponderPlugin(interceptor);
    }

    public ServerRSocketFactory addResponderPlugin(RSocketInterceptor interceptor) {
      plugins.addResponderPlugin(interceptor);
      return this;
    }

    public ServerRSocketFactory addSocketAcceptorPlugin(SocketAcceptorInterceptor interceptor) {
      plugins.addSocketAcceptorPlugin(interceptor);
      return this;
    }

    public ServerTransportAcceptor acceptor(SocketAcceptor acceptor) {
      this.acceptor = acceptor;
      return new ServerStart<>();
    }

    public ServerRSocketFactory frameDecoder(PayloadDecoder payloadDecoder) {
      this.payloadDecoder = payloadDecoder;
      return this;
    }

    public ServerRSocketFactory fragment(int mtu) {
      this.mtu = mtu;
      return this;
    }

    public ServerRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
      this.errorConsumer = errorConsumer;
      return this;
    }

    public ServerRSocketFactory lease(Supplier<Leases<?>> leasesSupplier) {
      this.leaseEnabled = true;
      this.leasesSupplier = Objects.requireNonNull(leasesSupplier);
      return this;
    }

    public ServerRSocketFactory lease() {
      this.leaseEnabled = true;
      return this;
    }

    public ServerRSocketFactory singleSubscriberRequester() {
      this.multiSubscriberRequester = false;
      return this;
    }

    public ServerRSocketFactory resume() {
      this.resumeSupported = true;
      return this;
    }

    public ServerRSocketFactory resumeStore(
        Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory) {
      this.resumeStoreFactory = resumeStoreFactory;
      return this;
    }

    public ServerRSocketFactory resumeSessionDuration(Duration sessionDuration) {
      this.resumeSessionDuration = Objects.requireNonNull(sessionDuration);
      return this;
    }

    public ServerRSocketFactory resumeStreamTimeout(Duration resumeStreamTimeout) {
      this.resumeStreamTimeout = Objects.requireNonNull(resumeStreamTimeout);
      return this;
    }

    public ServerRSocketFactory resumeCleanupOnKeepAlive() {
      resumeCleanupStoreOnKeepAlive = true;
      return this;
    }

    private class ServerStart<T extends Closeable> implements Start<T>, ServerTransportAcceptor {
      private Supplier<ServerTransport<T>> transportServer;

      @Override
      public ServerTransport.ConnectionAcceptor toConnectionAcceptor() {
        return new ServerTransport.ConnectionAcceptor() {
          private final ServerSetup serverSetup = serverSetup();

          @Override
          public Mono<Void> apply(DuplexConnection connection) {
            return acceptor(serverSetup, connection);
          }
        };
      }

      @Override
      @SuppressWarnings("unchecked")
      public <T extends Closeable> Start<T> transport(Supplier<ServerTransport<T>> transport) {
        this.transportServer = (Supplier) transport;
        return (Start) this::start;
      }

      private Mono<Void> acceptor(ServerSetup serverSetup, DuplexConnection connection) {
        ClientServerInputMultiplexer multiplexer =
            new ClientServerInputMultiplexer(connection, plugins, false);

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
            return acceptUnknown(startFrame, multiplexer);
        }
      }

      private Mono<Void> acceptSetup(
          ServerSetup serverSetup, ByteBuf setupFrame, ClientServerInputMultiplexer multiplexer) {

        if (!SetupFrameFlyweight.isSupportedVersion(setupFrame)) {
          return sendError(
                  multiplexer,
                  new InvalidSetupException(
                      "Unsupported version: "
                          + SetupFrameFlyweight.humanReadableVersion(setupFrame)))
              .doFinally(
                  signalType -> {
                    setupFrame.release();
                    multiplexer.dispose();
                  });
        }

        boolean isLeaseEnabled = leaseEnabled;

        if (SetupFrameFlyweight.honorLease(setupFrame) && !isLeaseEnabled) {
          return sendError(multiplexer, new InvalidSetupException("lease is not supported"))
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
              ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create(setupFrame);

              Leases<?> leases = leasesSupplier.get();
              RequesterLeaseHandler requesterLeaseHandler =
                  isLeaseEnabled
                      ? new RequesterLeaseHandler.Impl(SERVER_TAG, leases.receiver())
                      : RequesterLeaseHandler.None;

              RSocket rSocketRequester =
                  new RSocketRequester(
                      allocator,
                      wrappedMultiplexer.asServerConnection(),
                      payloadDecoder,
                      errorConsumer,
                      StreamIdSupplier.serverSupplier(),
                      setupPayload.keepAliveInterval(),
                      setupPayload.keepAliveMaxLifetime(),
                      keepAliveHandler,
                      requesterLeaseHandler);

              if (multiSubscriberRequester) {
                rSocketRequester = new MultiSubscriberRSocket(rSocketRequester);
              }
              RSocket wrappedRSocketRequester = plugins.applyRequester(rSocketRequester);

              return plugins
                  .applySocketAcceptorInterceptor(acceptor)
                  .accept(setupPayload, wrappedRSocketRequester)
                  .onErrorResume(
                      err -> sendError(multiplexer, rejectedSetupError(err)).then(Mono.error(err)))
                  .doOnNext(
                      rSocketHandler -> {
                        RSocket wrappedRSocketHandler = plugins.applyResponder(rSocketHandler);

                        ResponderLeaseHandler responderLeaseHandler =
                            isLeaseEnabled
                                ? new ResponderLeaseHandler.Impl<>(
                                    SERVER_TAG,
                                    allocator,
                                    leases.sender(),
                                    errorConsumer,
                                    leases.stats())
                                : ResponderLeaseHandler.None;

                        RSocket rSocketResponder =
                            new RSocketResponder(
                                allocator,
                                wrappedMultiplexer.asClientConnection(),
                                wrappedRSocketHandler,
                                payloadDecoder,
                                errorConsumer,
                                responderLeaseHandler);
                      })
                  .doFinally(signalType -> setupPayload.release())
                  .then();
            });
      }

      @Override
      public Mono<T> start() {
        return Mono.defer(
            new Supplier<Mono<T>>() {

              ServerSetup serverSetup = serverSetup();

              @Override
              public Mono<T> get() {
                return Mono.fromSupplier(transportServer)
                    .flatMap(
                        transport ->
                            transport.start(
                                duplexConnection -> acceptor(serverSetup, duplexConnection), mtu))
                    .doOnNext(c -> c.onClose().doFinally(v -> serverSetup.dispose()).subscribe());
              }
            });
      }

      private ServerSetup serverSetup() {
        return resumeSupported
            ? new ServerSetup.ResumableServerSetup(
                allocator,
                new SessionManager(),
                resumeSessionDuration,
                resumeStreamTimeout,
                resumeStoreFactory,
                resumeCleanupStoreOnKeepAlive)
            : new ServerSetup.DefaultServerSetup(allocator);
      }

      private Mono<Void> acceptUnknown(ByteBuf frame, ClientServerInputMultiplexer multiplexer) {
        return sendError(
                multiplexer,
                new InvalidSetupException(
                    "invalid setup frame: " + FrameHeaderFlyweight.frameType(frame)))
            .doFinally(
                signalType -> {
                  frame.release();
                  multiplexer.dispose();
                });
      }

      private Mono<Void> sendError(ClientServerInputMultiplexer multiplexer, Exception exception) {
        return ConnectionUtils.sendError(allocator, multiplexer, exception);
      }

      private Exception rejectedSetupError(Throwable err) {
        String msg = err.getMessage();
        return new RejectedSetupException(msg == null ? "rejected by server acceptor" : msg);
      }
    }
  }
}
