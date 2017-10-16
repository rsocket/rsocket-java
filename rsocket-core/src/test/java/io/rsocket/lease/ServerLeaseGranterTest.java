package io.rsocket.lease;

import static io.rsocket.lease.LeaseGranterTestUtils.newLease;
import static org.junit.Assert.*;

import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.test.util.TestDuplexConnection;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ServerLeaseGranterTest {

  private TestDuplexConnection testDuplexConnection;
  private LeaseManager requesterLeaseManager;
  private LeaseManager responderLeaseManager;
  private LeaseGranter leaseGranter;
  private int numberOfRequests;
  private int ttl;

  @Before
  public void setUp() throws Exception {
    numberOfRequests = 1;
    ttl = 1_000;
    testDuplexConnection = new TestDuplexConnection();
    requesterLeaseManager = new LeaseManager("requester");
    responderLeaseManager = new LeaseManager("responder");
    leaseGranter =
        LeaseGranter.ofServer(
            testDuplexConnection, requesterLeaseManager, responderLeaseManager, err -> {});
  }

  @Test
  public void grantLease() throws Exception {
    leaseGranter.grantLease(numberOfRequests, ttl, null);

    Lease responderLease = responderLeaseManager.getLease();
    assertTrue(responderLease.isValid());
    assertEquals(numberOfRequests, responderLease.getAllowedRequests());
    assertEquals(ttl, responderLease.getTtl());

    Lease requesterLease = requesterLeaseManager.getLease();
    assertFalse(requesterLease.isValid());

    Collection<Frame> sent = testDuplexConnection.getSent();
    Assert.assertEquals(1, sent.size());
    Frame frame = sent.iterator().next();
    Assert.assertEquals(FrameType.LEASE, frame.getType());
    Assert.assertEquals(numberOfRequests, Frame.Lease.numberOfRequests(frame));
    Assert.assertEquals(ttl, Frame.Lease.ttl(frame));
  }

  @Test
  public void receiveLeaseBeforeGrantLease() throws Exception {
    leaseGranter.grantedLeasesReceiver().accept(newLease(1, 1_000));
    LeaseImpl requesterLease = requesterLeaseManager.getLease();
    assertFalse(requesterLease.isValid());

    Collection<Frame> sent = testDuplexConnection.getSent();
    Assert.assertEquals(1, sent.size());
    Frame frame = sent.iterator().next();
    Assert.assertEquals(FrameType.ERROR, frame.getType());
    Assert.assertEquals(Frame.Error.ErrorCodes.REJECTED_SETUP, Frame.Error.errorCode(frame));
  }
}
