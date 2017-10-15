/*
package io.rsocket.lease;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class LeaseManagerTest {

  private LeaseManager leaseManager;
  private ByteBuffer buffer;
  private int allowedRequests;
  private int ttl;
  private Lease lease;

  @Before
  public void setUp() throws Exception {
    leaseManager = new LeaseManager("test");
    buffer = ByteBuffer.allocate(1);
    allowedRequests = 42;
    ttl = 1_000;
    lease = new LeaseImpl(allowedRequests, ttl, buffer);
  }

  @Test
  public void noLeases() throws Exception {
    Lease lease = leaseManager.getLeases().blockFirst();
    assertThat("no leases yet should produce invalid lease", !lease.isValid());
  }

  @Test
  public void leaseGranted() throws Exception {

    StepVerifier.create(
            leaseManager
                .getLeases()
                .publishOn(Schedulers.elastic())
                .doOnSubscribe(
                    s ->
                        Flux.interval(ofMillis(100))
                            .take(1)
                            .subscribe(ev -> leaseManager.leaseGranted(lease)))
                .take(2))
        .consumeNextWith(this::assertLeaseInvalid)
        .consumeNextWith(l -> assertLeaseEquals(lease, l))
        .expectComplete()
        .verify(ofSeconds(1));
  }

  @Test
  public void leaseGrantUsed() throws Exception {

    StepVerifier.create(
            leaseManager
                .getLeases()
                .publishOn(Schedulers.elastic())
                .doOnSubscribe(
                    s ->
                        Flux.interval(ofMillis(100))
                            .take(1)
                            .subscribe(
                                ev -> {
                                  leaseManager.leaseGranted(lease);
                                  leaseManager.useLease(1);
                                }))
                .take(3))
        .consumeNextWith(this::assertLeaseInvalid)
        .consumeNextWith(l -> assertLeaseEquals(lease, l))
        .consumeNextWith(l -> assertLeaseEquals(new LeaseImpl(allowedRequests - 1, ttl, buffer), l))
        .expectComplete()
        .verify(ofSeconds(1));
  }

  @Test
  public void subscribeAfterLeaseGrant() throws Exception {
    leaseManager.leaseGranted(lease);
    StepVerifier.create(leaseManager.getLeases().take(1))
        .consumeNextWith(l -> assertLeaseEquals(lease, l))
        .expectComplete()
        .verify(ofSeconds(1));
  }

  @Test
  public void leaseLossIsNotSignalledIfCurrentLeaseIsInvalid() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(1);
    LeaseImpl leaseLoss = new LeaseImpl(0, 1_000, buffer);

    Mono<Long> twoSecondsDelay = Flux.interval(Duration.ofSeconds(2)).next();
    StepVerifier.create(
            leaseManager
                .getLeases()
                .takeUntilOther(twoSecondsDelay)
                .publishOn(Schedulers.elastic())
                .doOnSubscribe(
                    s ->
                        Flux.interval(ofMillis(100))
                            .take(1)
                            .subscribe(
                                ev -> {
                                  leaseManager.leaseGranted(leaseLoss);
                                  leaseManager.leaseGranted(leaseLoss);
                                })))
        .consumeNextWith(this::assertLeaseInvalid)
        .expectNoEvent(Duration.ofSeconds(1))
        .expectComplete()
        .verify(ofSeconds(3));
  }

  @Test
  public void connectionCompleteCompleteLeases() throws Exception {
    Mono<Void> completeSignal =
        Mono.<Void>create(MonoSink::success).delaySubscription(Duration.ofMillis(500));
    LeaseManager leaseManager = new LeaseManager("test", completeSignal);
    leaseManager.leaseGranted(1, 1000, null);
    StepVerifier.create(leaseManager.getLeases())
        .consumeNextWith(l -> assertThat("start with valid lease", l.isValid()))
        .consumeNextWith(l -> assertThat("after completion is invalid", !l.isValid()))
        .verifyComplete();
  }

  @Test
  public void grantLeaseAfterCompletionIsNoop() throws Exception {
    Mono<Void> completeSignal =
        Mono.<Void>create(MonoSink::success).delaySubscription(Duration.ofMillis(500));
    LeaseManager leaseManager = new LeaseManager("test", completeSignal);
    leaseManager.leaseGranted(1, 1000, null);
    Mono.delay(Duration.ofSeconds(1)).subscribe(signal -> leaseManager.leaseGranted(1, 1000, null));
    StepVerifier.create(leaseManager.getLeases())
        .consumeNextWith(l -> assertThat("start with valid lease", l.isValid()))
        .consumeNextWith(l -> assertThat("after completion is invalid", !l.isValid()))
        .verifyComplete();
  }

  private void assertLeaseInvalid(Lease l) {
    assertThat("start with invalid lease", !l.isValid());
    assertThat("start with expired lease", l.isExpired());
    assertThat("start with no requests lease", l.getAllowedRequests() == 0);
  }

  private void assertLeaseEquals(Lease lease, Lease l) {
    assertThat(
        "allowed requests is equal", l.getAllowedRequests(), equalTo(lease.getAllowedRequests()));
    assertThat("timetolive is equal", l.getTtl(), equalTo(lease.getTtl()));
  }
}
*/
