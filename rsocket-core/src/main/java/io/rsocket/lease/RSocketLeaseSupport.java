package io.rsocket.lease;

import io.rsocket.DuplexConnection;
import io.rsocket.RSocket;
import io.rsocket.plugins.RSocketInterceptor;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Utility used to enable lease for single connection */
class RSocketLeaseSupport {
  private final LeaseManager requesterLeaseManager;
  private final LeaseManager responderLeaseManager;
  private final LeaseGranter leaseGranter;

  RSocketLeaseSupport(
      LeaseManager requesterLeaseManager,
      LeaseManager responderLeaseManager,
      Flux<Lease> receivedLease,
      LeaseGranter leaseGranter) {
    this.leaseGranter = leaseGranter;
    this.requesterLeaseManager = requesterLeaseManager;
    this.responderLeaseManager = responderLeaseManager;
    receivedLease.subscribe(leaseGranter.grantedLeasesReceiver());
  }

  static RSocketLeaseSupport create(
      @Nonnull DuplexConnection senderConnection,
      @Nonnull Flux<Lease> receivedLease,
      @Nonnull Consumer<Throwable> errorConsumer,
      LeaseSupport.LeaseGranterFactory leaseGranterFactory) {
    Mono<Void> connectionClose = senderConnection.onClose();
    LeaseManager requesterLeaseManager = new LeaseManager("requester", connectionClose);
    LeaseManager responderLeaseManager = new LeaseManager("responder", connectionClose);
    return new RSocketLeaseSupport(
        requesterLeaseManager,
        responderLeaseManager,
        receivedLease,
        leaseGranterFactory.apply(
            senderConnection, requesterLeaseManager, responderLeaseManager, errorConsumer));
  }

  LeaseRSocketRef getRSocketRef(RSocket rSocket) {
    return new LeaseRSocketRef(leaseGranter, rSocket);
  }

  RSocketInterceptor getRequesterInterceptor() {
    return rSocket -> new LeaseRSocket(rSocket, requesterLeaseManager, "requester");
  }

  RSocketInterceptor getResponderInterceptor() {
    return rSocket -> new LeaseRSocket(rSocket, responderLeaseManager, "responder");
  }
}
