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
import io.rsocket.Closeable;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
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
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.PluginRegistry;
import io.rsocket.plugins.Plugins;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.plugins.SocketAcceptorInterceptor;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.resume.ResumableFramesStore;
import io.rsocket.resume.SessionManager;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.MultiSubscriberRSocket;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/**
 * Default implementation of {@link RSocketFactory.ServerRSocketFactory} that can be instantiated
 * directly or through the shortcut {@link RSocketFactory#receive()}.
 */
public class DefaultServerRSocketFactory implements RSocketFactory.ServerRSocketFactory {
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

  @Override
  public RSocketFactory.ServerRSocketFactory byteBufAllocator(ByteBufAllocator allocator) {
    Objects.requireNonNull(allocator);
    this.allocator = allocator;
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory addConnectionPlugin(
      DuplexConnectionInterceptor interceptor) {
    plugins.addConnectionPlugin(interceptor);
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory addRequesterPlugin(RSocketInterceptor interceptor) {
    plugins.addRequesterPlugin(interceptor);
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory addResponderPlugin(RSocketInterceptor interceptor) {
    plugins.addResponderPlugin(interceptor);
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory addSocketAcceptorPlugin(
      SocketAcceptorInterceptor interceptor) {
    plugins.addSocketAcceptorPlugin(interceptor);
    return this;
  }

  @Override
  public RSocketFactory.ServerTransportAcceptor acceptor(SocketAcceptor acceptor) {
    this.acceptor = acceptor;
    return new ServerStart<>();
  }

  @Override
  public RSocketFactory.ServerRSocketFactory frameDecoder(PayloadDecoder payloadDecoder) {
    this.payloadDecoder = payloadDecoder;
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory fragment(int mtu) {
    this.mtu = mtu;
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
    this.errorConsumer = errorConsumer;
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory lease(Supplier<Leases<?>> leasesSupplier) {
    this.leaseEnabled = true;
    this.leasesSupplier = Objects.requireNonNull(leasesSupplier);
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory lease() {
    this.leaseEnabled = true;
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory singleSubscriberRequester() {
    this.multiSubscriberRequester = false;
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory resume() {
    this.resumeSupported = true;
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory resumeStore(
      Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory) {
    this.resumeStoreFactory = resumeStoreFactory;
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory resumeSessionDuration(Duration sessionDuration) {
    this.resumeSessionDuration = Objects.requireNonNull(sessionDuration);
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory resumeStreamTimeout(Duration resumeStreamTimeout) {
    this.resumeStreamTimeout = Objects.requireNonNull(resumeStreamTimeout);
    return this;
  }

  @Override
  public RSocketFactory.ServerRSocketFactory resumeCleanupOnKeepAlive() {
    resumeCleanupStoreOnKeepAlive = true;
    return this;
  }

  private class ServerStart<T extends Closeable>
      implements RSocketFactory.Start, RSocketFactory.ServerTransportAcceptor {
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
    public <T extends Closeable> RSocketFactory.Start<T> transport(
        Supplier<ServerTransport<T>> transport) {
      this.transportServer = (Supplier) transport;
      return (RSocketFactory.Start) this::start;
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

      boolean isLeaseEnabled = leaseEnabled;

      if (SetupFrameFlyweight.honorLease(setupFrame) && !isLeaseEnabled) {
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
                    err ->
                        serverSetup
                            .sendError(multiplexer, rejectedSetupError(err))
                            .then(Mono.error(err)))
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

    private Exception rejectedSetupError(Throwable err) {
      String msg = err.getMessage();
      return new RejectedSetupException(msg == null ? "rejected by server acceptor" : msg);
    }
  }
}
