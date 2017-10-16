package io.rsocket.lease;

import io.rsocket.DuplexConnection;
import io.rsocket.plugins.PluginRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import reactor.core.publisher.Flux;

/** Facade used to enable lease for Client or Server */
public class LeaseSupport {
  private static final PluginRegistry emptyPluginRegistry = new PluginRegistry();
  private static final Function<DuplexConnection, LeaseEnabled> disabled =
          conn -> new LeaseEnabled(emptyPluginRegistry,conn);

  private final LeaseRSocketRegistry leaseRSocketRegistry = new LeaseRSocketRegistry();
  private final Consumer<Throwable> errorConsumer;
  private final Consumer<LeaseControl> leaseControlConsumer;
  private final LeaseGranterFactory factory;
  private final AtomicBoolean leaseControlConsumed = new AtomicBoolean();

  LeaseSupport(
      Consumer<Throwable> errorConsumer,
      Consumer<LeaseControl> leaseControlConsumer,
      LeaseGranterFactory factory) {
    this.errorConsumer = errorConsumer;
    this.leaseControlConsumer = leaseControlConsumer;
    this.factory = factory;
  }

  public static LeaseSupport ofClient(
      Consumer<Throwable> errorConsumer, Consumer<LeaseControl> leaseControlConsumer) {
    return new LeaseSupport(errorConsumer, leaseControlConsumer, LeaseGranter::ofClient);
  }

  public static LeaseSupport ofServer(
      Consumer<Throwable> errorConsumer, Consumer<LeaseControl> leaseControlConsumer) {
    return new LeaseSupport(errorConsumer, leaseControlConsumer, LeaseGranter::ofServer);
  }

  public static LeaseEnabled disable(DuplexConnection clientConnection) {
    return disabled.apply(clientConnection);
  }

  public LeaseEnabled enable(DuplexConnection clientConnection) {
    /*notify consumer with LeaseControl on first connection*/
    if (leaseControlConsumed.compareAndSet(false, true)) {
      leaseControlConsumer.accept(new LeaseControl(leaseRSocketRegistry));
    }
    LeaseListenerConnection listenerConnection = new LeaseListenerConnection(clientConnection);
    Flux<Lease> leaseReceivedFlux = listenerConnection.leaseReceived();
    RSocketLeaseSupport rsocketLeaseSupport =
        RSocketLeaseSupport.create(clientConnection, leaseReceivedFlux, errorConsumer, factory);
    PluginRegistry interceptors = new PluginRegistry();
    /*make Requester and Responder RSockets respect lease*/
    interceptors.addClientPlugin(rsocketLeaseSupport.getRequesterInterceptor());
    interceptors.addServerPlugin(rsocketLeaseSupport.getResponderInterceptor());
    /*add  RSocketRef to LeaseControl*/
    interceptors.addServerPlugin(
        rsocket -> {
          leaseRSocketRegistry.addLeaseRSocket(rsocketLeaseSupport.getRSocketRef(rsocket));
          return rsocket;
        });
    return new LeaseEnabled(interceptors, listenerConnection);
  }

  interface LeaseGranterFactory {
    LeaseGranter apply(
        DuplexConnection senderConnection,
        LeaseManager requesterLeaseManager,
        LeaseManager responderLeaseManager,
        Consumer<Throwable> errorConsumer);
  }
}
