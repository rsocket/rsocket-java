package io.rsocket.lease;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.plugins.RSocketInterceptor;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import reactor.core.publisher.Mono;

/** Facade used to enable lease support in client and server rsocket factories */
public class LeaseSupport {
  private final LeaseManager requesterLeaseManager;
  private final LeaseManager responderLeaseManager;
  private final LeaseGranter leaseGranter;

  LeaseSupport(
      LeaseManager requesterLeaseManager,
      LeaseManager responderLeaseManager,
      LeaseGranter leaseGranter) {
    this.leaseGranter = leaseGranter;
    this.requesterLeaseManager = requesterLeaseManager;
    this.responderLeaseManager = responderLeaseManager;
  }

  public static LeaseSupport ofServer(
      @Nonnull DuplexConnection senderConnection, Consumer<Throwable> errorConsumer) {
    Mono<Void> connectionClose = senderConnection.onClose();
    LeaseManager requesterLeaseManager = new LeaseManager("requester", connectionClose);
    LeaseManager responderLeaseManager = new LeaseManager("responder", connectionClose);
    return new LeaseSupport(
        requesterLeaseManager,
        responderLeaseManager,
        LeaseGranter.ofServer(
            senderConnection, requesterLeaseManager, responderLeaseManager, errorConsumer));
  }

  public static LeaseSupport ofClient(
      DuplexConnection senderConnection, Consumer<Throwable> errorConsumer) {
    Mono<Void> connectionClose = senderConnection.onClose();
    LeaseManager requesterLeaseManager = new LeaseManager("requester", connectionClose);
    LeaseManager responderLeaseManager = new LeaseManager("responder", connectionClose);
    return new LeaseSupport(
        requesterLeaseManager,
        responderLeaseManager,
        LeaseGranter.ofClient(
            senderConnection, requesterLeaseManager, responderLeaseManager, errorConsumer));
  }

  public Consumer<Frame> getLeaseReceiver() {
    return leaseGranter.grantedLeasesReceiver();
  }

  public RSocketInterceptor getRequesterEnforcer() {
    return rsocket -> new LeaseEnforcingRSocket(rsocket, requesterLeaseManager, "requester");
  }

  public RSocketInterceptor getResponderEnforcer() {
    return rsocket -> new LeaseEnforcingRSocket(rsocket, responderLeaseManager, "responder");
  }

  public LeaseControl getLeaseControl() {
    return new LeaseControl(requesterLeaseManager, leaseGranter);
  }
}
