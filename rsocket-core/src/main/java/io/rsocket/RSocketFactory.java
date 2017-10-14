/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket;

import io.rsocket.exceptions.InvalidSetupException;
import io.rsocket.exceptions.SetupException;
import io.rsocket.exceptions.UnsupportedSetupException;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.frame.VersionFlyweight;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.lease.*;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.PluginRegistry;
import io.rsocket.plugins.Plugins;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

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

  public interface SetupPayload<T> {
    T setupPayload(Payload payload);
  }

  public interface Acceptor<T, A> {
    T acceptor(Supplier<A> acceptor);

    default T acceptor(A acceptor) {
      return acceptor(() -> acceptor);
    }
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

  public interface Fragmentation<T> {
    T fragment(int mtu);
  }

  public interface ErrorConsumer<T> {
    T errorConsumer(Consumer<Throwable> errorConsumer);
  }

  public interface KeepAlive<T> {
    T keepAlive();

    T keepAlive(Duration tickPeriod, Duration ackTimeout, int missedAcks);

    T keepAliveTickPeriod(Duration tickPeriod);

    T keepAliveAckTimeout(Duration ackTimeout);

    T keepAliveMissedAcks(int missedAcks);
  }

  public interface MimeType<T> {
    T mimeType(String metadataMimeType, String dataMimeType);

    T dataMimeType(String dataMimeType);

    T metadataMimeType(String metadataMimeType);
  }

  public interface Leasing<T> {
    T enableLease(Consumer<LeaseControl> leaseControlConsumer);

    T disableLease();
  }

  public static class ClientRSocketFactory
      implements Acceptor<ClientTransportAcceptor, Function<RSocket, RSocket>>,
          ClientTransportAcceptor,
          KeepAlive<ClientRSocketFactory>,
          MimeType<ClientRSocketFactory>,
          Fragmentation<ClientRSocketFactory>,
          ErrorConsumer<ClientRSocketFactory>,
          SetupPayload<ClientRSocketFactory>,
          Leasing<ClientRSocketFactory> {

    private Supplier<Function<RSocket, RSocket>> acceptor =
        () -> rSocket -> new AbstractRSocket() {};
    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private int mtu = 0;
    private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());
    private int flags = SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;

    private Payload setupPayload = PayloadImpl.EMPTY;

    private Duration tickPeriod = Duration.ZERO;
    private Duration ackTimeout = Duration.ofSeconds(30);
    private int missedAcks = 3;

    private String metadataMimeType = "application/binary";
    private String dataMimeType = "application/binary";
    private Optional<Consumer<LeaseControl>> leaseControlConsumer = Optional.empty();

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

    @Override
    public ClientRSocketFactory keepAlive() {
      tickPeriod = Duration.ofSeconds(20);
      return this;
    }

    @Override
    public ClientRSocketFactory keepAlive(
        Duration tickPeriod, Duration ackTimeout, int missedAcks) {
      this.tickPeriod = tickPeriod;
      this.ackTimeout = ackTimeout;
      this.missedAcks = missedAcks;
      return this;
    }

    @Override
    public ClientRSocketFactory keepAliveTickPeriod(Duration tickPeriod) {
      this.tickPeriod = tickPeriod;
      return this;
    }

    @Override
    public ClientRSocketFactory keepAliveAckTimeout(Duration ackTimeout) {
      this.ackTimeout = ackTimeout;
      return this;
    }

    @Override
    public ClientRSocketFactory keepAliveMissedAcks(int missedAcks) {
      this.missedAcks = missedAcks;
      return this;
    }

    @Override
    public ClientRSocketFactory mimeType(String metadataMimeType, String dataMimeType) {
      this.dataMimeType = dataMimeType;
      this.metadataMimeType = metadataMimeType;
      return this;
    }

    @Override
    public ClientRSocketFactory dataMimeType(String dataMimeType) {
      this.dataMimeType = dataMimeType;
      return this;
    }

    @Override
    public ClientRSocketFactory metadataMimeType(String metadataMimeType) {
      this.metadataMimeType = metadataMimeType;
      return this;
    }

    @Override
    public Start<RSocket> transport(Supplier<ClientTransport> transportClient) {
      return new StartClient(transportClient);
    }

    @Override
    public ClientTransportAcceptor acceptor(Supplier<Function<RSocket, RSocket>> acceptor) {
      this.acceptor = acceptor;
      return StartClient::new;
    }

    @Override
    public ClientRSocketFactory fragment(int mtu) {
      this.mtu = mtu;
      return this;
    }

    @Override
    public ClientRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
      this.errorConsumer = errorConsumer;
      return this;
    }

    @Override
    public ClientRSocketFactory setupPayload(Payload payload) {
      this.setupPayload = payload;
      return this;
    }

    @Override
    public ClientRSocketFactory enableLease(Consumer<LeaseControl> leaseControlConsumer) {
      this.leaseControlConsumer = Optional.of(leaseControlConsumer);
      flags |= SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE;
      return this;
    }

    @Override
    public ClientRSocketFactory disableLease() {
      this.leaseControlConsumer = Optional.empty();
      flags &= ~SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE;
      return this;
    }

    protected class StartClient implements Start<RSocket> {
      private final Supplier<ClientTransport> transportClient;
      private final Optional<LeaseSupport> leaseSupport =
          leaseControlConsumer.map(
              leaseCtrlConsumer -> new LeaseSupport(errorConsumer, leaseCtrlConsumer));

      StartClient(Supplier<ClientTransport> transportClient) {
        this.transportClient = transportClient;
      }

      @Override
      public Mono<RSocket> start() {
        return transportClient
            .get()
            .connect()
            .flatMap(
                connection -> {
                  Frame setupFrame =
                      Frame.Setup.from(
                          flags,
                          (int) ackTimeout.toMillis(),
                          (int) ackTimeout.toMillis() * missedAcks,
                          metadataMimeType,
                          dataMimeType,
                          setupPayload);

                  if (mtu > 0) {
                    connection = new FragmentationDuplexConnection(connection, mtu);
                  }
                  ClientServerInputMultiplexer multiplexer =
                      new ClientServerInputMultiplexer(connection, plugins);

                  PluginRegistry localPlugins = new PluginRegistry();
                  DuplexConnection clientConnection =
                      leaseSupport
                          .map(
                              leaseSupp ->
                                  leaseSupp.wrap(localPlugins, multiplexer.asClientConnection()))
                          .orElseGet(multiplexer::asClientConnection);

                  RSocketRequester rSocketRequester =
                      new RSocketRequester(
                          clientConnection,
                          errorConsumer,
                          StreamIdSupplier.clientSupplier(),
                          tickPeriod,
                          ackTimeout,
                          missedAcks);

                  Mono<RSocket> wrappedRSocketClient =
                      Mono.just(rSocketRequester)
                          .map(plugins::applyClient)
                          .map(localPlugins::applyClient);
                  DuplexConnection finalConnection = connection;

                  return wrappedRSocketClient.flatMap(
                      wrappedClientRSocket -> {
                        RSocket unwrappedServerSocket = acceptor.get().apply(wrappedClientRSocket);
                        Mono<RSocket> wrappedRSocketServer =
                            Mono.just(unwrappedServerSocket)
                                .map(plugins::applyServer)
                                .map(localPlugins::applyServer);

                        return wrappedRSocketServer
                            .doOnNext(
                                rSocket ->
                                    new RSocketResponder(
                                        multiplexer.asServerConnection(), rSocket, errorConsumer))
                            .then(finalConnection.sendOne(setupFrame))
                            .then(wrappedRSocketClient);
                      });
                });
      }
    }
  }

  public static class ServerRSocketFactory
      implements Acceptor<ServerTransportAcceptor, SocketAcceptor>,
          Fragmentation<ServerRSocketFactory>,
          ErrorConsumer<ServerRSocketFactory>,
          Leasing<ServerRSocketFactory> {

    private Supplier<SocketAcceptor> acceptor;
    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private int mtu = 0;
    private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());
    private Optional<Consumer<LeaseControl>> leaseControlConsumer = Optional.empty();

    private ServerRSocketFactory() {}

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

    @Override
    public ServerTransportAcceptor acceptor(Supplier<SocketAcceptor> acceptor) {
      this.acceptor = acceptor;
      return ServerStart::new;
    }

    @Override
    public ServerRSocketFactory fragment(int mtu) {
      this.mtu = mtu;
      return this;
    }

    @Override
    public ServerRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
      this.errorConsumer = errorConsumer;
      return this;
    }

    @Override
    public ServerRSocketFactory enableLease(Consumer<LeaseControl> leaseControlConsumer) {
      this.leaseControlConsumer = Optional.of(leaseControlConsumer);
      return this;
    }

    @Override
    public ServerRSocketFactory disableLease() {
      this.leaseControlConsumer = Optional.empty();
      return this;
    }

    private class ServerStart<T extends Closeable> implements Start<T> {
      private final Supplier<ServerTransport<T>> transportServer;
      private final Optional<LeaseSupport> leaseSupport =
          leaseControlConsumer.map(
              leaseCtrlConsumer -> new LeaseSupport(errorConsumer, leaseCtrlConsumer));

      ServerStart(Supplier<ServerTransport<T>> transportServer) {
        this.transportServer = transportServer;
      }

      @Override
      public Mono<T> start() {
        return transportServer
            .get()
            .start(
                connection -> {
                  if (mtu > 0) {
                    connection = new FragmentationDuplexConnection(connection, mtu);
                  }
                  ClientServerInputMultiplexer multiplexer =
                      new ClientServerInputMultiplexer(connection, plugins);

                  return multiplexer
                      .asStreamZeroConnection()
                      .receive()
                      .next()
                      .flatMap(setupFrame -> processSetupFrame(multiplexer, setupFrame));
                });
      }

      private Mono<Void> processSetupFrame(
          ClientServerInputMultiplexer multiplexer, Frame setupFrame) {
        int version = Frame.Setup.version(setupFrame);
        if (version != SetupFrameFlyweight.CURRENT_VERSION) {
          InvalidSetupException error =
              new InvalidSetupException(
                  "Unsupported version " + VersionFlyweight.toString(version));
          return setupError(multiplexer, error);
        }

        if (Frame.Setup.supportsLease(setupFrame) && !serverLeaseEnabled()) {
          UnsupportedSetupException error =
              new UnsupportedSetupException("Server does not support lease");
          return setupError(multiplexer, error);
        }

        PluginRegistry localPlugins = new PluginRegistry();
        DuplexConnection clientConnection =
            Frame.Setup.supportsLease(setupFrame)
                ? leaseSupport.get().wrap(localPlugins, multiplexer.asClientConnection())
                : multiplexer.asClientConnection();

        RSocketRequester rSocketRequester =
            new RSocketRequester(
                multiplexer.asServerConnection(), errorConsumer, StreamIdSupplier.serverSupplier());

        Mono<RSocket> wrappedRSocketClient =
            Mono.just(rSocketRequester).map(plugins::applyClient).map(localPlugins::applyClient);

        ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create(setupFrame);

        return wrappedRSocketClient
            .flatMap(
                sender ->
                    acceptor
                        .get()
                        .accept(setupPayload, sender)
                        .map(plugins::applyServer)
                        .map(localPlugins::applyServer))
            .map(handler -> new RSocketResponder(clientConnection, handler, errorConsumer))
            .then();
      }

      private boolean serverLeaseEnabled() {
        return leaseSupport.isPresent();
      }

      Mono<Void> setupError(ClientServerInputMultiplexer multiplexer, SetupException error) {
        return multiplexer
            .asStreamZeroConnection()
            .sendOne(Frame.Error.from(0, error))
            .then(multiplexer.close());
      }
    }
  }
}
