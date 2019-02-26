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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.exceptions.InvalidSetupException;
import io.rsocket.exceptions.RejectedResumeException;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.exceptions.UnsupportedSetupException;
import io.rsocket.frame.*;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.keepalive.KeepAliveConnection;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.PluginRegistry;
import io.rsocket.plugins.Plugins;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.resume.*;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.ClientSetup;
import io.rsocket.util.EmptyPayload;
import io.rsocket.util.KeepAliveData;
import io.rsocket.util.ServerSetup;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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
    <T extends Closeable> Start<T> transport(Supplier<ServerTransport<T>> transport);

    default <T extends Closeable> Start<T> transport(ServerTransport<T> transport) {
      return transport(() -> transport);
    }
  }

  public static class ClientRSocketFactory implements ClientTransportAcceptor {
    private Supplier<Function<RSocket, RSocket>> acceptor =
        () -> rSocket -> new AbstractRSocket() {};

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
    private Supplier<ResumeToken> resumeTokenSupplier = ResumeToken::generate;
    private long resumeCacheSize = 32768;
    private Duration resumeSessionDuration = Duration.ofMinutes(2);
    private Supplier<ResumeStrategy> resumeStrategySupplier =
        () ->
            new ExponentialBackoffResumeStrategy(Duration.ofSeconds(1), Duration.ofSeconds(16), 2);
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

    public ClientRSocketFactory addClientPlugin(RSocketInterceptor interceptor) {
      plugins.addClientPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory addServerPlugin(RSocketInterceptor interceptor) {
      plugins.addServerPlugin(interceptor);
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

    public ClientRSocketFactory resume() {
      this.resumeEnabled = true;
      return this;
    }

    public ClientRSocketFactory resumeToken(Supplier<ResumeToken> resumeTokenSupplier) {
      this.resumeTokenSupplier = Objects.requireNonNull(resumeTokenSupplier);
      return this;
    }

    public ClientRSocketFactory resumeCacheSize(long framesCount) {
      this.resumeCacheSize = assertResumeCacheSize(framesCount);
      return this;
    }

    public ClientRSocketFactory resumeSessionDuration(Duration sessionDuration) {
      this.resumeSessionDuration = Objects.requireNonNull(sessionDuration);
      return this;
    }

    public ClientRSocketFactory resumeStrategy(Supplier<ResumeStrategy> resumeStrategy) {
      this.resumeStrategySupplier = Objects.requireNonNull(resumeStrategy);
      return this;
    }

    @Override
    public Start<RSocket> transport(Supplier<ClientTransport> transportClient) {
      return new StartClient(transportClient);
    }

    public ClientTransportAcceptor acceptor(Function<RSocket, RSocket> acceptor) {
      this.acceptor = () -> acceptor;
      return StartClient::new;
    }

    public ClientTransportAcceptor acceptor(Supplier<Function<RSocket, RSocket>> acceptor) {
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
                  ClientSetup clientSetup = clientSetup();
                  DuplexConnection wrappedConnection = clientSetup.wrappedConnection(connection);

                  ClientServerInputMultiplexer multiplexer =
                      new ClientServerInputMultiplexer(wrappedConnection, plugins);

                  RSocketClient rSocketClient =
                      new RSocketClient(
                          allocator,
                          multiplexer.asClientConnection(),
                          payloadDecoder,
                          errorConsumer,
                          StreamIdSupplier.clientSupplier());

                  RSocket wrappedRSocketClient = plugins.applyClient(rSocketClient);

                  RSocket unwrappedServerSocket = acceptor.get().apply(wrappedRSocketClient);

                  RSocket wrappedRSocketServer = plugins.applyServer(unwrappedServerSocket);

                  RSocketServer rSocketServer =
                      new RSocketServer(
                          allocator,
                          multiplexer.asServerConnection(),
                          wrappedRSocketServer,
                          payloadDecoder,
                          errorConsumer);

                  ByteBuf setupFrame =
                      SetupFrameFlyweight.encode(
                          allocator,
                          false,
                          (int) keepAliveTickPeriod(),
                          (int) keepAliveTimeout(),
                          clientSetup.resumeToken().toByteBuf(),
                          metadataMimeType,
                          dataMimeType,
                          setupPayload.sliceMetadata(),
                          setupPayload.sliceData());

                  return connection.sendOne(setupFrame).thenReturn(wrappedRSocketClient);
                });
      }

      private long keepAliveTickPeriod() {
        return tickPeriod.toMillis();
      }

      private long keepAliveTimeout() {
        return ackTimeout.toMillis() + tickPeriod.toMillis() * missedAcks;
      }

      private ClientSetup clientSetup() {
        return resumeEnabled
            ? new ResumableClientSetup(newConnection())
            : new DefaultClientSetup();
      }

      private Mono<KeepAliveConnection> newConnection() {
        return transportClient
            .get()
            .connect(mtu)
            .map(
                connection ->
                    KeepAliveConnection.ofClient(
                        allocator,
                        connection,
                        notUsed ->
                            Mono.just(
                                new KeepAliveData(
                                    keepAliveTickPeriod(),
                                    keepAliveTimeout())
                            ),
                        errorConsumer));
      }

      class DefaultClientSetup implements ClientSetup {

        @Override
        public DuplexConnection wrappedConnection(KeepAliveConnection connection) {
          return connection;
        }

        @Override
        public ResumeToken resumeToken() {
          return ResumeToken.empty();
        }
      }

      private class ResumableClientSetup implements ClientSetup {
        private final ResumeToken resumeToken;
        private ClientResumeConfiguration config;
        private Mono<KeepAliveConnection> newConnection;

        public ResumableClientSetup(Mono<KeepAliveConnection> newConnection) {
          this.newConnection = newConnection;
          this.resumeToken = resumeTokenSupplier.get();
          this.config =
              new ClientResumeConfiguration(
                  resumeSessionDuration, resumeCacheSize, resumeStrategySupplier);
        }

        @Override
        public DuplexConnection wrappedConnection(KeepAliveConnection connection) {
          ClientRSocketSession rSocketSession =
              new ClientRSocketSession(allocator, connection, config)
                  .continueWith(newConnection)
                  .resumeWith(resumeToken);

          return rSocketSession.resumableConnection();
        }

        @Override
        public ResumeToken resumeToken() {
          return resumeToken;
        }
      }
    }
  }

  public static class ServerRSocketFactory {
    private SocketAcceptor acceptor;
    private PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;
    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private int mtu = 0;
    private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());
    private boolean resumeSupported;
    private Duration resumeSessionDuration = Duration.ofSeconds(120);
    private long resumeCacheSize = 32768;
    private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

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

    public ServerRSocketFactory addClientPlugin(RSocketInterceptor interceptor) {
      plugins.addClientPlugin(interceptor);
      return this;
    }

    public ServerRSocketFactory addServerPlugin(RSocketInterceptor interceptor) {
      plugins.addServerPlugin(interceptor);
      return this;
    }

    public ServerTransportAcceptor acceptor(SocketAcceptor acceptor) {
      this.acceptor = acceptor;
      return ServerStart::new;
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

    public ServerRSocketFactory resume() {
      this.resumeSupported = true;
      return this;
    }

    public ServerRSocketFactory resumeCacheSize(long framesCount) {
      this.resumeCacheSize = assertResumeCacheSize(framesCount);
      return this;
    }

    public ServerRSocketFactory resumeSessionDuration(Duration sessionDuration) {
      this.resumeSessionDuration = Objects.requireNonNull(sessionDuration);
      return this;
    }

    private class ServerStart<T extends Closeable> implements Start<T> {
      private final Supplier<ServerTransport<T>> transportServer;

      ServerStart(Supplier<ServerTransport<T>> transportServer) {
        this.transportServer = transportServer;
      }

      @Override
      public Mono<T> start() {
        return Mono.defer(new Supplier<Mono<T>>() {

          ServerSetup serverSetup = serverSetup();

          @Override
          public Mono<T> get() {
            return transportServer
                .get()
                .start(
                    connection -> {
                      connection =
                          KeepAliveConnection.ofServer(
                              allocator,
                              connection,
                              serverSetup.keepAliveData(),
                              errorConsumer);
                      ClientServerInputMultiplexer multiplexer =
                          new ClientServerInputMultiplexer(connection, plugins);

                      return multiplexer
                          .asSetupConnection()
                          .receive()
                          .next()
                          .flatMap(startFrame -> accept(startFrame, multiplexer));
                    },
                    mtu)
                .doOnNext(c -> c.onClose().doFinally(v -> serverSetup.dispose()).subscribe());
          }

          private Mono<Void> accept(
              ByteBuf startFrame, ClientServerInputMultiplexer multiplexer) {
            switch (FrameHeaderFlyweight.frameType(startFrame)) {
              case SETUP:
                return setupRSocket(startFrame, multiplexer);
              case RESUME:
                return resumeRSocket(startFrame, multiplexer);
              default:
                return invalidSetupRSocket(startFrame, multiplexer);
            }
          }

          private Mono<Void> setupRSocket(
              ByteBuf setupFrame, ClientServerInputMultiplexer multiplexer) {

            if (!SetupFrameFlyweight.isSupportedVersion(setupFrame)) {
              return errorThenDispose(
                  setupFrame,
                  f ->
                      new InvalidSetupException(
                          "Unsupported version: "
                              + SetupFrameFlyweight.humanReadableVersion(f)),
                  multiplexer);
            }
            return serverSetup.setup(
                setupFrame,
                multiplexer,
                wrappedMultiplexer -> {
                  ConnectionSetupPayload setupPayload =
                      ConnectionSetupPayload.create(setupFrame);

                  RSocketClient rSocketClient =
                      new RSocketClient(
                          allocator,
                          wrappedMultiplexer.asServerConnection(),
                          payloadDecoder,
                          errorConsumer,
                          StreamIdSupplier.serverSupplier());

                  RSocket wrappedRSocketClient = plugins.applyClient(rSocketClient);

                  return acceptor
                      .accept(setupPayload, wrappedRSocketClient)
                      .onErrorResume(
                          err ->
                              wrappedMultiplexer
                                  .asSetupConnection()
                                  .sendOne(rejectedSetupError(err))
                                  .then(Mono.error(err)))
                      .doOnNext(
                          unwrappedServerSocket -> {
                            RSocket wrappedRSocketServer =
                                plugins.applyServer(unwrappedServerSocket);

                            RSocketServer rSocketServer =
                                new RSocketServer(
                                    allocator,
                                    wrappedMultiplexer.asClientConnection(),
                                    wrappedRSocketServer,
                                    payloadDecoder,
                                    errorConsumer);
                          })
                      .doFinally(signalType -> setupPayload.release())
                      .then();
                });
          }

          private Mono<Void> resumeRSocket(
              ByteBuf resumeFrame, ClientServerInputMultiplexer multiplexer) {
            return serverSetup.resume(resumeFrame, multiplexer);
          }
        });
      }

      private class ResumableServerSetup implements ServerSetup {
        private final SessionManager sessionManager;
        private final ServerResumeConfiguration resumeConfig;

        public ResumableServerSetup(SessionManager sessionManager) {
          this.sessionManager = sessionManager;
          this.resumeConfig = new ServerResumeConfiguration(resumeSessionDuration, resumeCacheSize);
        }

        @Override
        public Mono<Void> setup(
            ByteBuf frame,
            ClientServerInputMultiplexer multiplexer,
            Function<ClientServerInputMultiplexer, Mono<Void>> then) {

          ResumeToken token = ResumeToken.fromBytes(SetupFrameFlyweight.resumeToken(frame));
          if (!token.isEmpty()) {

            KeepAliveData keepAliveData =
                new KeepAliveData(
                    SetupFrameFlyweight.keepAliveInterval(frame),
                    SetupFrameFlyweight.keepAliveMaxLifetime(frame));

            DuplexConnection resumableConnection =
                sessionManager
                    .save(
                        new ServerRSocketSession(
                            allocator,
                            multiplexer.asClientServerConnection(),
                            resumeConfig,
                            keepAliveData,
                            token))
                    .resumableConnection();
            return then.apply(new ClientServerInputMultiplexer(resumableConnection));
          } else {
            return then.apply(multiplexer);
          }
        }

        @Override
        public Mono<Void> resume(ByteBuf frame, ClientServerInputMultiplexer multiplexer) {
          return sessionManager
              .get(ResumeToken.fromBytes(ResumeFrameFlyweight.token(frame)))
              .map(
                  session ->
                      session
                          .continueWith(multiplexer.asClientServerConnection())
                          .resumeWith(frame)
                          .onClose()
                          .then())
              .orElseGet(
                  () ->
                      multiplexer
                          .asSetupConnection()
                          .sendOne(
                              ErrorFrameFlyweight.encode(allocator,
                                  0,
                                  new RejectedResumeException("unknown resume token")))
                          .onErrorResume(err -> Mono.empty())
                          .doFinally(
                              s -> {
                                frame.release();
                                multiplexer.dispose();
                              })
                          .then());
        }

        @Override
        public Function<ByteBuf, Mono<KeepAliveData>> keepAliveData() {
          return frame -> {
            if (FrameHeaderFlyweight.frameType(frame) == FrameType.SETUP) {
              return Mono.just(
                  new KeepAliveData(
                      SetupFrameFlyweight.keepAliveInterval(frame),
                      SetupFrameFlyweight.keepAliveMaxLifetime(frame)));
            } else {
              return sessionManager
                  .get(ResumeToken.fromBytes(ResumeFrameFlyweight.token(frame)))
                  .map(ServerRSocketSession::keepAliveData)
                  .map(Mono::just)
                  .orElseGet(Mono::never);
            }
          };
        }

        @Override
        public void dispose() {
          sessionManager.dispose();
        }
      }

      private class DefaultServerSetup implements ServerSetup {

        @Override
        public Mono<Void> setup(
            ByteBuf frame,
            ClientServerInputMultiplexer multiplexer,
            Function<ClientServerInputMultiplexer, Mono<Void>> then) {

          if (!ResumeToken.fromBytes(SetupFrameFlyweight.resumeToken(frame)).isEmpty()) {
            return errorThenDispose(
                frame, f -> new UnsupportedSetupException("resume not supported"), multiplexer);
          } else {
            return then.apply(multiplexer);
          }
        }

        @Override
        public Mono<Void> resume(ByteBuf frame, ClientServerInputMultiplexer multiplexer) {

          return errorThenDispose(
              frame, f -> new RejectedResumeException("resume not supported"), multiplexer);
        }

        @Override
        public Function<ByteBuf, Mono<KeepAliveData>> keepAliveData() {
          return frame -> {
            if (FrameHeaderFlyweight.frameType(frame) == FrameType.SETUP) {
              return Mono.just(
                  new KeepAliveData(
                      SetupFrameFlyweight.keepAliveInterval(frame),
                      SetupFrameFlyweight.keepAliveMaxLifetime(frame)));
            } else {
              return Mono.never();
            }
          };
        }
      }

      private ServerSetup serverSetup() {
        return resumeSupported
            ? new ResumableServerSetup(new SessionManager())
            : new DefaultServerSetup();
      }

      private Mono<Void> invalidSetupRSocket(
          ByteBuf frame,
          ClientServerInputMultiplexer multiplexer) {
        return errorThenDispose(
            frame,
            f -> new InvalidSetupException("invalid setup frame: " + FrameHeaderFlyweight.frameType(f)),
            multiplexer);
      }

      private Mono<Void> errorThenDispose(
          ByteBuf frame,
          Function<ByteBuf, Exception> exception,
          ClientServerInputMultiplexer multiplexer) {
        return multiplexer
            .asSetupConnection()
            .sendOne(ErrorFrameFlyweight.encode(allocator, 0, exception.apply(frame)))
            .onErrorResume(err -> Mono.empty())
            .doFinally(
                signalType -> {
                  frame.release();
                  multiplexer.dispose();
                });
      }

      private ByteBuf rejectedSetupError(Throwable err) {
        String msg = err.getMessage();
        return ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            0,
            new RejectedSetupException(msg == null ? "rejected by server acceptor" : msg));
      }
    }
  }

  private static long assertResumeCacheSize(long cacheSize) {
    if (cacheSize <= 0) {
      throw new IllegalArgumentException(
          "Resume cache size should be positive: " + cacheSize);
    } else {
      return cacheSize;
    }
  }
}
