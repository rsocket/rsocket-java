package io.rsocket.lease;

import io.rsocket.DuplexConnection;
import io.rsocket.RSocket;
import io.rsocket.plugins.RSocketInterceptor;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Facade used to enable lease in client and server rsocket factories, per connection */
public class RSocketLeaseSupport {
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

  public static RSocketLeaseSupport ofServer(
      @Nonnull DuplexConnection senderConnection,
      @Nonnull Flux<Lease> receivedLease,
      @Nonnull Consumer<Throwable> errorConsumer) {
    Mono<Void> connectionClose = senderConnection.onClose();
    LeaseManager requesterLeaseManager = new LeaseManager("requester", connectionClose);
    LeaseManager responderLeaseManager = new LeaseManager("responder", connectionClose);
    return new RSocketLeaseSupport(
        requesterLeaseManager,
        responderLeaseManager,
        receivedLease,
        LeaseGranter.ofServer(
            senderConnection, requesterLeaseManager, responderLeaseManager, errorConsumer));
  }

  public static RSocketLeaseSupport ofClient(
      DuplexConnection senderConnection,
      Flux<Lease> receivedLease,
      Consumer<Throwable> errorConsumer) {
    Mono<Void> connectionClose = senderConnection.onClose();
    LeaseManager requesterLeaseManager = new LeaseManager("requester", connectionClose);
    LeaseManager responderLeaseManager = new LeaseManager("responder", connectionClose);
    return new RSocketLeaseSupport(
        requesterLeaseManager,
        responderLeaseManager,
        receivedLease,
        LeaseGranter.ofClient(
            senderConnection, requesterLeaseManager, responderLeaseManager, errorConsumer));
  }

  public Function<RSocket, LeaseRSocketRef> responderRefFactory() {
    return rSocket -> new LeaseRSocketRef(leaseGranter, rSocket);
  }

  public RSocketInterceptor getRequesterInterceptor() {
    return rSocket -> new LeaseRSocket(rSocket, requesterLeaseManager, "requester");
  }

  public RSocketInterceptor getResponderInterceptor() {
    return rSocket -> new LeaseRSocket(rSocket, responderLeaseManager, "responder");
  }
}
