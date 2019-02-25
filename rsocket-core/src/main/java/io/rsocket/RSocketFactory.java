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
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.frame.ErrorFrameFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.frame.VersionFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.keepalive.KeepAliveConnection;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.PluginRegistry;
import io.rsocket.plugins.Plugins;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.EmptyPayload;
import io.rsocket.util.KeepAliveData;
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
                  ByteBuf setupFrame =
                      SetupFrameFlyweight.encode(
                          allocator,
                          false,
                          false,
                          (int) tickPeriod.toMillis(),
                          (int) (ackTimeout.toMillis() + tickPeriod.toMillis() * missedAcks),
                          metadataMimeType,
                          dataMimeType,
                          setupPayload.sliceMetadata(),
                          setupPayload.sliceData());

                  ClientServerInputMultiplexer multiplexer =
                      new ClientServerInputMultiplexer(connection, plugins);

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

                  return connection.sendOne(setupFrame).thenReturn(wrappedRSocketClient);
                });
      }

      private long keepAliveTickPeriod() {
        return tickPeriod.toMillis();
      }

      private long keepAliveTimeout() {
        return ackTimeout.toMillis() + tickPeriod.toMillis() * missedAcks;
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
    }
  }

  public static class ServerRSocketFactory {
    private SocketAcceptor acceptor;
    private PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;
    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private int mtu = 0;
    private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());
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

    private class ServerStart<T extends Closeable> implements Start<T> {
      private final Supplier<ServerTransport<T>> transportServer;

      ServerStart(Supplier<ServerTransport<T>> transportServer) {
        this.transportServer = transportServer;
      }

      @Override
      public Mono<T> start() {
        return transportServer
            .get()
            .start(
                connection -> {
                  connection =
                      KeepAliveConnection.ofServer(
                          allocator,
                          connection,
                          keepAliveData(),
                          errorConsumer);
                  ClientServerInputMultiplexer multiplexer =
                      new ClientServerInputMultiplexer(connection, plugins);

                  return multiplexer
                      .asStreamZeroConnection()
                      .receive()
                      .next()
                      .flatMap(setupFrame -> processSetupFrame(multiplexer, setupFrame));
                },
                mtu);
      }

      private Function<ByteBuf, Mono<KeepAliveData>> keepAliveData() {
        return frame -> Mono.just(
            new KeepAliveData(
                SetupFrameFlyweight.keepAliveInterval(frame),
                SetupFrameFlyweight.keepAliveMaxLifetime(frame)));
      }

      private Mono<Void> processSetupFrame(
          ClientServerInputMultiplexer multiplexer, ByteBuf setupFrame) {
        int version = SetupFrameFlyweight.version(setupFrame);
        if (version != SetupFrameFlyweight.CURRENT_VERSION) {
          setupFrame.release();
          InvalidSetupException error =
              new InvalidSetupException(
                  "Unsupported version " + VersionFlyweight.toString(version));
          return multiplexer
              .asStreamZeroConnection()
              .sendOne(ErrorFrameFlyweight.encode(ByteBufAllocator.DEFAULT, 0, error))
              .doFinally(signalType -> multiplexer.dispose());
        }

        ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create(setupFrame);

        RSocketClient rSocketClient =
            new RSocketClient(
                allocator,
                multiplexer.asServerConnection(),
                payloadDecoder,
                errorConsumer,
                StreamIdSupplier.serverSupplier());

        RSocket wrappedRSocketClient = plugins.applyClient(rSocketClient);

        return acceptor
            .accept(setupPayload, wrappedRSocketClient)
            .onErrorResume(
                err ->
                    multiplexer
                        .asStreamZeroConnection()
                        .sendOne(rejectedSetupErrorFrame(err))
                        .then(Mono.error(err)))
            .doOnNext(
                unwrappedServerSocket -> {
                  RSocket wrappedRSocketServer = plugins.applyServer(unwrappedServerSocket);

                  RSocketServer rSocketServer =
                      new RSocketServer(
                          allocator,
                          multiplexer.asClientConnection(),
                          wrappedRSocketServer,
                          payloadDecoder,
                          errorConsumer);
                })
            .doFinally(signalType -> setupPayload.release())
            .then();
      }

      private ByteBuf rejectedSetupErrorFrame(Throwable err) {
        String msg = err.getMessage();
        return ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            0,
            new RejectedSetupException(msg == null ? "rejected by server acceptor" : msg));
      }
    }
  }
}
