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

public class ClientLeaseGranterTest {

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
        LeaseGranter.ofClient(
            testDuplexConnection, requesterLeaseManager, responderLeaseManager, err -> {});
  }

  @Test
  public void grantLeaseWithoutServerLeaseReceived() throws Exception {

    leaseGranter.grantLease(numberOfRequests, ttl, null);

    Lease responderLease = responderLeaseManager.getLease();
    Lease requesterLease = requesterLeaseManager.getLease();
    assertFalse("on lease grant responder lease is invalid", responderLease.isValid());
    assertFalse(requesterLease.isValid());

    Collection<Frame> sent = testDuplexConnection.getSent();
    Assert.assertEquals(0, sent.size());
  }

  @Test
  public void grantLeaseBeforeServerLeaseReceived() throws Exception {

    leaseGranter.grantLease(numberOfRequests, ttl, null);
    leaseGranter.grantedLeasesReceiver().accept(newLease(numberOfRequests, ttl));

    Lease requesterLease = requesterLeaseManager.getLease();
    assertTrue("on lease grant requester lease is valid", requesterLease.isValid());
    assertEquals(
        "on lease grant requester lease numberOfRequests is consistent",
        numberOfRequests,
        requesterLease.getAllowedRequests());
    assertEquals(
        "on lease grant requester lease timeToLive is consistent", ttl, requesterLease.getTtl());

    Lease responderLease = responderLeaseManager.getLease();
    assertTrue("on lease grant responder lease is valid", responderLease.isValid());
    assertEquals(
        "on lease grant responder lease numberOfRequests is consistent",
        numberOfRequests,
        responderLease.getAllowedRequests());
    int actualTtl = responderLease.getTtl();
    assertTrue("on lease grant responder lease timeToLive is delayed", actualTtl <= ttl);
    assertTrue("on lease grant responder lease timeToLive is present", actualTtl > 0);

    Collection<Frame> sent = testDuplexConnection.getSent();
    Assert.assertEquals(1, sent.size());
    Frame frame = sent.iterator().next();
    Assert.assertEquals(FrameType.LEASE, frame.getType());
    Assert.assertEquals(numberOfRequests, Frame.Lease.numberOfRequests(frame));
    Assert.assertEquals(actualTtl, Frame.Lease.ttl(frame));
  }

  @Test
  public void grantLeaseAfterServerLeaseReceived() throws Exception {

    leaseGranter.grantedLeasesReceiver().accept(newLease(numberOfRequests, ttl));
    leaseGranter.grantLease(numberOfRequests, ttl, null);

    Lease requesterLease = requesterLeaseManager.getLease();
    assertTrue("on lease grant requester lease is valid", requesterLease.isValid());
    assertEquals(
        "on lease grant requester lease numberOfRequests is consistent",
        numberOfRequests,
        requesterLease.getAllowedRequests());
    assertEquals(
        "on lease grant requester lease timeToLive is consistent", ttl, requesterLease.getTtl());

    Lease responderLease = responderLeaseManager.getLease();
    assertTrue("on lease grant responder lease is valid", responderLease.isValid());
    assertEquals(
        "on lease grant responder lease numberOfRequests is consistent",
        numberOfRequests,
        responderLease.getAllowedRequests());
    assertEquals(
        "on lease grant responder lease timeToLive is consistent", ttl, responderLease.getTtl());

    Collection<Frame> sent = testDuplexConnection.getSent();
    Assert.assertEquals(1, sent.size());
    Frame frame = sent.iterator().next();
    Assert.assertEquals(FrameType.LEASE, frame.getType());
    Assert.assertEquals(numberOfRequests, Frame.Lease.numberOfRequests(frame));
    Assert.assertEquals(ttl, Frame.Lease.ttl(frame));
  }
}
