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
import io.rsocket.lease.LeaseControl;
import io.rsocket.lease.LeaseSupport;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.PluginRegistry;
import io.rsocket.plugins.Plugins;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.PayloadImpl;
import io.rsocket.util.RSocketProxy;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

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

  public interface ClientTransportLeaseAcceptor {
    Start<LeaseRSocket> transport(Supplier<ClientTransport> transport);

    default Start<LeaseRSocket> transport(ClientTransport transport) {
      return transport(() -> transport);
    }
  }

  public interface ServerTransportAcceptor {
    <T extends Closeable> Start<T> transport(Supplier<ServerTransport<T>> transport);

    default <T extends Closeable> Start<T> transport(ServerTransport<T> transport) {
      return transport(() -> transport);
    }
  }

  public interface ServerTransportLeaseAcceptor {
    <T extends Closeable> ServerRSocketFactory.LeaseServerStart<T> transport(
        Supplier<ServerTransport<T>> transport);

    default <T extends Closeable> ServerRSocketFactory.LeaseServerStart<T> transport(
        ServerTransport<T> transport) {
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

  public static class ClientRSocketFactory
      implements Acceptor<ClientTransportAcceptor, Function<RSocket, RSocket>>,
          ClientTransportAcceptor,
          KeepAlive<ClientRSocketFactory>,
          MimeType<ClientRSocketFactory>,
          Fragmentation<ClientRSocketFactory>,
          ErrorConsumer<ClientRSocketFactory>,
          SetupPayload<ClientRSocketFactory> {

    private Supplier<Function<RSocket, RSocket>> acceptor =
        () -> rSocket -> new AbstractRSocket() {};
    private Supplier<Function<LeaseRSocket, RSocket>> leaseAcceptor =
        () -> rSocket -> new AbstractRSocket() {};
    private boolean hasLeaseAcceptor;
    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private int mtu = 0;
    private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());
    private int flags = SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;

    private Payload setupPayload = PayloadImpl.EMPTY;

    private Duration tickPeriod = Duration.ZERO;
    private Duration ackTimeout = Duration.ofSeconds(30);
    private int missedAcks = 3;
    private boolean clientLeaseEnabled;

    private String metadataMimeType = "application/binary";
    private String dataMimeType = "application/binary";

    public ClientRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      plugins.addConnectionPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory addClientPlugin(RSocketInterceptor interceptor) {
      plugins.addRequesterPlugin(interceptor);
      return this;
    }

    public ClientRSocketFactory addServerPlugin(RSocketInterceptor interceptor) {
      plugins.addResponderPlugin(interceptor);
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
      hasLeaseAcceptor = false;
      return StartClient::new;
    }

    public ClientTransportLeaseAcceptor leaseAcceptor(Function<LeaseRSocket, RSocket> acceptor) {
      this.leaseAcceptor = () -> acceptor;
      hasLeaseAcceptor = true;
      return LeaseStartClient::new;
    }

    public ClientTransportLeaseAcceptor emptyLeaseAcceptor() {
      return leaseAcceptor(rsocket -> new AbstractRSocket() {});
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

    public ClientRSocketFactory enableLease() {
      flags |= SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE;
      this.clientLeaseEnabled = true;
      return this;
    }

    public ClientRSocketFactory disableLease() {
      flags = flags & ~SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE;
      this.clientLeaseEnabled = false;
      return this;
    }

    public class LeaseStartClient implements Start<LeaseRSocket> {
      private final Supplier<ClientTransport> transportClient;

      public LeaseStartClient(Supplier<ClientTransport> transportClient) {
        this.transportClient = transportClient;
      }

      @Override
      public Mono<LeaseRSocket> start() {
        return new StartClient(transportClient).startWithLease();
      }
    }

    protected class StartClient implements Start<RSocket> {
      private final Supplier<ClientTransport> transportClient;

      StartClient(Supplier<ClientTransport> transportClient) {
        this.transportClient = transportClient;
      }

      public Mono<LeaseRSocket> startWithLease() {
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

                  Optional<LeaseSupport> maybeLeaseSupport =
                      clientLeaseEnabled
                          ? Optional.of(
                              LeaseSupport.ofClient(
                                  multiplexer.asClientConnection(), errorConsumer))
                          : Optional.empty();
                  Optional<LeaseControl> maybeLeaseControl =
                      maybeLeaseSupport.map(LeaseSupport::getLeaseControl);
                  Optional<Consumer<Frame>> maybeLeaseReceiver =
                      maybeLeaseSupport.map(LeaseSupport::getLeaseReceiver);
                  maybeLeaseSupport.ifPresent(
                      leaseSupport -> {
                        plugins.addRequesterPlugin(leaseSupport.getRequesterEnforcer());
                        plugins.addResponderPlugin(leaseSupport.getResponderEnforcer());
                      });

                  RSocketRequester rSocketRequester =
                      new RSocketRequester(
                          multiplexer.asClientConnection(),
                          errorConsumer,
                          StreamIdSupplier.clientSupplier(),
                          maybeLeaseReceiver,
                          tickPeriod,
                          ackTimeout,
                          missedAcks);

                  Mono<RSocket> wrappedRSocketClient =
                      Mono.just(rSocketRequester).map(plugins::applyRequester);
                  DuplexConnection finalConnection = connection;
                  Mono<RSocket> rsocket =
                      wrappedRSocketClient.flatMap(
                          wrappedClientRSocket -> {
                            RSocket unwrappedServerSocket =
                                hasLeaseAcceptor
                                    ? leaseAcceptor
                                        .get()
                                        .apply(
                                            new LeaseRSocketImpl(
                                                wrappedClientRSocket, maybeLeaseControl))
                                    : acceptor.get().apply(wrappedClientRSocket);

                            Mono<RSocket> wrappedRSocketServer =
                                Mono.just(unwrappedServerSocket).map(plugins::applyResponder);

                            return wrappedRSocketServer
                                .doOnNext(
                                    rSocket ->
                                        new RSocketResponder(
                                            multiplexer.asServerConnection(),
                                            rSocket,
                                            errorConsumer))
                                .then(finalConnection.sendOne(setupFrame))
                                .then(wrappedRSocketClient);
                          });
                  return rsocket.map(socket -> new LeaseRSocketImpl(socket, maybeLeaseControl));
                });
      }

      @Override
      public Mono<RSocket> start() {
        return startWithLease().map(RSocketProxy::new);
      }
    }
  }

  public static class ServerRSocketFactory
      implements Acceptor<ServerTransportAcceptor, SocketAcceptor>,
          Fragmentation<ServerRSocketFactory>,
          ErrorConsumer<ServerRSocketFactory> {

    private Supplier<SocketAcceptor> acceptor;
    private Supplier<LeaseSocketAcceptor> leaseSocketAcceptor;
    private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
    private int mtu = 0;
    private PluginRegistry plugins = new PluginRegistry(Plugins.defaultPlugins());
    private boolean serverLeaseEnabled;

    private ServerRSocketFactory() {}

    public ServerRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      plugins.addConnectionPlugin(interceptor);
      return this;
    }

    public ServerRSocketFactory addClientPlugin(RSocketInterceptor interceptor) {
      plugins.addRequesterPlugin(interceptor);
      return this;
    }

    public ServerRSocketFactory addServerPlugin(RSocketInterceptor interceptor) {
      plugins.addResponderPlugin(interceptor);
      return this;
    }

    @Override
    public ServerTransportAcceptor acceptor(Supplier<SocketAcceptor> acceptor) {
      this.acceptor = acceptor;
      this.leaseSocketAcceptor = null;
      return ServerStart::new;
    }

    public ServerTransportLeaseAcceptor leaseAcceptor(LeaseSocketAcceptor acceptor) {
      this.leaseSocketAcceptor = () -> acceptor;
      this.acceptor = null;
      return LeaseServerStart::new;
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

    public ServerRSocketFactory enableLease() {
      this.serverLeaseEnabled = true;
      return this;
    }

    public ServerRSocketFactory disableLease() {
      this.serverLeaseEnabled = false;
      return this;
    }

    public class LeaseServerStart<T extends Closeable> implements Start<LeaseClosable<T>> {
      Supplier<ServerTransport<T>> transportServer;

      public LeaseServerStart(Supplier<ServerTransport<T>> transportServer) {
        this.transportServer = transportServer;
      }

      @Override
      public Mono<LeaseClosable<T>> start() {
        return new ServerStart<>(transportServer).startWithLease();
      }
    }

    private class ServerStart<T extends Closeable> implements Start<T> {
      private final Supplier<ServerTransport<T>> transportServer;

      ServerStart(Supplier<ServerTransport<T>> transportServer) {
        this.transportServer = transportServer;
      }

      @Override
      public Mono<T> start() {
        return startWithLease().map(LeaseClosable::getCloseable);
      }

      public Mono<LeaseClosable<T>> startWithLease() {
        MonoProcessor<Optional<LeaseControl>> leaseControl = MonoProcessor.create();
        Mono<T> start =
            transportServer
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
                          .flatMap(
                              setupFrame ->
                                  processSetupFrame(multiplexer, setupFrame, leaseControl));
                    });
        return start.map(closable -> new LeaseClosable<>(closable, leaseControl));
      }

      private Mono<? extends Void> processSetupFrame(
          ClientServerInputMultiplexer multiplexer,
          Frame setupFrame,
          MonoProcessor<Optional<LeaseControl>> leaseControlMono) {
        int version = Frame.Setup.version(setupFrame);
        if (version != SetupFrameFlyweight.CURRENT_VERSION) {
          InvalidSetupException error =
              new InvalidSetupException(
                  "Unsupported version " + VersionFlyweight.toString(version));
          return setupError(multiplexer, error);
        }

        boolean clientLeaseEnabled = Frame.Setup.supportsLease(setupFrame);
        if (clientLeaseEnabled && !serverLeaseEnabled) {
          UnsupportedSetupException error =
              new UnsupportedSetupException("Server does not support lease");
          return setupError(multiplexer, error);
        }

        Optional<LeaseSupport> maybeLeaseSupport =
            clientLeaseEnabled
                ? Optional.of(
                    LeaseSupport.ofServer(multiplexer.asClientConnection(), errorConsumer))
                : Optional.empty();
        maybeLeaseSupport.ifPresent(
            leaseSupport -> {
              plugins.addRequesterPlugin(leaseSupport.getRequesterEnforcer());
              plugins.addResponderPlugin(leaseSupport.getResponderEnforcer());
            });
        Optional<Consumer<Frame>> leaseConsumer =
            maybeLeaseSupport.map(LeaseSupport::getLeaseReceiver);

        Optional<LeaseControl> maybeLeaseControl =
            maybeLeaseSupport.map(LeaseSupport::getLeaseControl);
        leaseControlMono.onNext(maybeLeaseControl);

        RSocketRequester rSocketRequester =
            new RSocketRequester(
                multiplexer.asServerConnection(), errorConsumer, StreamIdSupplier.serverSupplier());

        Mono<RSocket> wrappedRSocketClient = Mono.just(rSocketRequester).map(plugins::applyRequester);
        ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create(setupFrame);
        boolean hasLeaseAcceptor = leaseSocketAcceptor != null;
        return wrappedRSocketClient
            .flatMap(
                sender -> {
                  if (hasLeaseAcceptor) {
                    return leaseSocketAcceptor
                        .get()
                        .accept(setupPayload, new LeaseRSocketImpl(sender, maybeLeaseControl))
                        .map(plugins::applyResponder);
                  } else {
                    return acceptor.get().accept(setupPayload, sender).map(plugins::applyResponder);
                  }
                })
            .map(
                handler ->
                    new RSocketResponder(
                        multiplexer.asClientConnection(), handler, errorConsumer, leaseConsumer))
            .then();
      }

      private Mono<? extends Void> setupError(
          ClientServerInputMultiplexer multiplexer, SetupException error) {
        return multiplexer
            .asStreamZeroConnection()
            .sendOne(Frame.Error.from(0, error))
            .then(multiplexer.close());
      }
    }
  }
}
