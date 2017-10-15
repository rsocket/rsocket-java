/*
package io.rsocket.lease;

import static org.junit.Assert.*;

import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.test.util.TestDuplexConnection;
import java.time.Duration;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class ClientLeaseGranterTest extends LeaseGranterTest {

  private TestDuplexConnection testDuplexConnection;
  private LeaseManager requesterLeaseManager;
  private LeaseManager responderLeaseManager;
  private LeaseGranter leaseGranter;

  @Before
  public void setUp() throws Exception {
    testDuplexConnection = new TestDuplexConnection();
    requesterLeaseManager = new LeaseManager("requester");
    responderLeaseManager = new LeaseManager("responder");
    leaseGranter =
        LeaseGranter.ofClient(
            testDuplexConnection, requesterLeaseManager, responderLeaseManager, err -> {});
  }

  @Test
  public void grantLeaseBeforeLeaseReceived() throws Exception {
    Flux<Lease> responderLeases = timeLimitedLeases(responderLeaseManager, Duration.ofSeconds(2));
    Flux<Lease> requesterLeases = timeLimitedLeases(requesterLeaseManager, Duration.ofSeconds(2));

    int numberOfRequests = 1;
    int ttl = 1_000;
    delay(Duration.ofMillis(500))
        .subscribe(signal -> leaseGranter.grantLease(numberOfRequests, ttl, null));

    StepVerifier.create(responderLeases)
        .consumeNextWith(l -> assertFalse(l.isValid()))
        .verifyComplete();

    StepVerifier.create(requesterLeases)
        .consumeNextWith(l -> assertFalse(l.isValid()))
        .verifyComplete();

    Collection<Frame> sent = testDuplexConnection.getSent();
    Assert.assertEquals(0, sent.size());
  }

  @Test
  public void grantLeaseAfterLeaseReceived() throws Exception {
    Flux<Lease> responderLeases = timeLimitedLeases(responderLeaseManager, Duration.ofSeconds(2));
    Flux<Lease> requesterLeases = timeLimitedLeases(requesterLeaseManager, Duration.ofSeconds(2));

    int numberOfRequests = 1;
    int ttl = 1_000;
    leaseGranter.grantedLeasesReceiver().accept(newLeaseFrame(numberOfRequests, ttl));
    delay(Duration.ofMillis(500))
        .subscribe(signal -> leaseGranter.grantLease(numberOfRequests, ttl, null));

    StepVerifier.create(responderLeases)
        .consumeNextWith(l -> assertTrue(!l.isValid()))
        .consumeNextWith(
            l -> {
              assertEquals(numberOfRequests, l.getAllowedRequests());
              assertEquals(ttl, l.getTtl());
            })
        .verifyComplete();

    StepVerifier.create(requesterLeases)
        .consumeNextWith(l -> assertFalse(l.isValid()))
        .verifyComplete();

    Collection<Frame> sent = testDuplexConnection.getSent();
    Assert.assertEquals(1, sent.size());
    Frame frame = sent.iterator().next();
    Assert.assertEquals(FrameType.LEASE, frame.getType());
    Assert.assertEquals(numberOfRequests, Frame.Lease.numberOfRequests(frame));
    Assert.assertEquals(ttl, Frame.Lease.ttl(frame));
  }

  @Test
  public void pendingGrantLeaseAfterReceived() throws Exception {
    Flux<Lease> responderLeases = timeLimitedLeases(responderLeaseManager, Duration.ofSeconds(3));

    int numberOfRequests = 1;
    int ttl = 1_000;
    leaseGranter.grantLease(numberOfRequests, ttl, null);
    delay(Duration.ofMillis(500))
        .subscribe(
            signal ->
                leaseGranter.grantedLeasesReceiver().accept(newLeaseFrame(numberOfRequests, ttl)));

    StepVerifier.create(responderLeases)
        .consumeNextWith(l -> assertTrue(!l.isValid()))
        .consumeNextWith(
            l -> {
              assertEquals(numberOfRequests, l.getAllowedRequests());
              assertTrue(l.getTtl() < ttl);
            })
        .verifyComplete();

    Collection<Frame> sent = testDuplexConnection.getSent();
    Assert.assertEquals(1, sent.size());
    Frame frame = sent.iterator().next();
    Assert.assertEquals(FrameType.LEASE, frame.getType());
    Assert.assertEquals(numberOfRequests, Frame.Lease.numberOfRequests(frame));
  }

  @Test
  public void leaseReceived() throws Exception {
    Flux<Lease> responderLeases = timeLimitedLeases(responderLeaseManager, Duration.ofSeconds(2));
    Flux<Lease> requesterLeases = timeLimitedLeases(requesterLeaseManager, Duration.ofSeconds(2));

    int numberOfRequests = 1;
    int ttl = 1_000;
    delay(Duration.ofMillis(500))
        .subscribe(
            signal ->
                leaseGranter.grantedLeasesReceiver().accept(newLeaseFrame(numberOfRequests, ttl)));

    StepVerifier.create(requesterLeases)
        .consumeNextWith(l -> assertFalse(l.isValid()))
        .consumeNextWith(
            l -> {
              assertEquals(numberOfRequests, l.getAllowedRequests());
              assertEquals(ttl, l.getTtl());
            })
        .verifyComplete();

    StepVerifier.create(responderLeases)
        .consumeNextWith(l -> assertFalse(l.isValid()))
        .verifyComplete();
  }
}
*/
