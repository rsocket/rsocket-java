package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.exceptions.RejectedSetupException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

/**
 * Grants lease to requester rsocket for incoming LEASE frames, and responder rsocket for outgoing
 * LEASE frames
 */
abstract class LeaseGranter {
  protected final DuplexConnection senderConnection;
  private final LeaseManager requesterLeaseManager;
  private final LeaseManager responderLeaseManager;
  protected Consumer<Throwable> errorConsumer;

  LeaseGranter(
      @NonNull DuplexConnection senderConnection,
      @NonNull LeaseManager requesterLeaseManager,
      @NonNull LeaseManager responderLeaseManager,
      @Nonnull Consumer<Throwable> errorConsumer) {
    this.senderConnection = senderConnection;
    this.requesterLeaseManager = requesterLeaseManager;
    this.responderLeaseManager = responderLeaseManager;
    this.errorConsumer = errorConsumer;
  }

  static LeaseGranter ofServer(
      @NonNull DuplexConnection senderConnection,
      @NonNull LeaseManager requesterLeaseManager,
      @NonNull LeaseManager responderLeaseManager,
      @Nonnull Consumer<Throwable> errorConsumer) {
    return new ServerLeaseGranter(
        senderConnection, requesterLeaseManager, responderLeaseManager, errorConsumer);
  }

  static LeaseGranter ofClient(
      @NonNull DuplexConnection senderConnection,
      @NonNull LeaseManager requesterLeaseManager,
      @NonNull LeaseManager responderLeaseManager,
      @Nonnull Consumer<Throwable> errorConsumer) {
    return new ClientLeaseGranter(
        senderConnection, requesterLeaseManager, responderLeaseManager, errorConsumer);
  }

  abstract Consumer<Lease> grantedLeasesReceiver();

  abstract void grantLease(int numberOfRequests, int timeToLive, @Nullable ByteBuffer metadata);

  void leaseReceived(@Nonnull Lease lease) {
    requesterLeaseManager.leaseGranted(lease);
  }

  void sendLease(Lease lease) {
    ByteBuffer metadata = lease.getMetadata();
    ByteBuf byteBuf = metadata == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(metadata);
    responderLeaseManager.leaseGranted(lease);
    senderConnection
        .sendOne(Frame.Lease.from(lease.getTtl(), lease.getAllowedRequests(), byteBuf))
        .subscribe(ignored -> {}, errorConsumer);
  }

  private static class ClientLeaseGranter extends LeaseGranter {
    private boolean leaseReceived;
    private Lease pendingSentLease;

    ClientLeaseGranter(
        DuplexConnection senderConnection,
        LeaseManager requesterLeaseManager,
        LeaseManager responderLeaseManager,
        Consumer<Throwable> errorConsumer) {
      super(senderConnection, requesterLeaseManager, responderLeaseManager, errorConsumer);
    }

    @Override
    Consumer<Lease> grantedLeasesReceiver() {
      return f -> {
        synchronized (this) {
          leaseReceived = true;
          if (pendingSentLease != null) {
            Lease lease = pendingSentLease;
            pendingSentLease = null;
            long now = System.currentTimeMillis();
            int newTTl = (int) Math.max(lease.expiry() - now, 0);
            sendLease(new LeaseImpl(lease.getAllowedRequests(), newTTl, lease.getMetadata()));
          }
          leaseReceived(f);
        }
      };
    }

    @Override
    void grantLease(int numberOfRequests, int timeToLive, ByteBuffer metadata) {
      LeaseImpl lease = new LeaseImpl(numberOfRequests, timeToLive, metadata);
      synchronized (this) {
        if (!leaseReceived) {
          pendingSentLease = lease;
        } else {
          sendLease(lease);
        }
      }
    }
  }

  private static class ServerLeaseGranter extends LeaseGranter {
    private boolean validState = true;
    private boolean leaseGranted;

    ServerLeaseGranter(
        DuplexConnection senderConnection,
        LeaseManager requesterLeaseManager,
        LeaseManager responderLeaseManager,
        Consumer<Throwable> errorConsumer) {
      super(senderConnection, requesterLeaseManager, responderLeaseManager, errorConsumer);
    }

    @Override
    Consumer<Lease> grantedLeasesReceiver() {
      return f -> {
        synchronized (this) {
          if (!leaseGranted) {
            validState = false;
            RejectedSetupException exception =
                new RejectedSetupException(
                    "Received client LEASE frame before sending server LEASE frame");
            senderConnection
                .sendOne(Frame.Error.from(0, exception))
                .then(senderConnection.close())
                .subscribe(ignored -> {}, errorConsumer);
          } else {
            leaseReceived(f);
          }
        }
      };
    }

    @Override
    void grantLease(int numberOfRequests, int timeToLive, @Nullable ByteBuffer metadata) {
      synchronized (this) {
        if (validState) {
          leaseGranted = true;
          sendLease(new LeaseImpl(numberOfRequests, timeToLive, metadata));
        }
      }
    }
  }
}
