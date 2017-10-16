package io.rsocket.lease;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;

public class LeaseManagerTest {

  private LeaseManager leaseManager;
  private ByteBuffer buffer;
  private int allowedRequests;
  private int ttl;
  private LeaseImpl lease;

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
    Lease lease = leaseManager.getLease();
    assertThat("no leases yet should produce invalid lease", !lease.isValid());
  }

  @Test
  public void leaseGranted() throws Exception {
    leaseManager.leaseGranted(lease);
    LeaseImpl actual = leaseManager.getLease();
    assertThat("lease is valid", lease.isValid());
    assertThat(
        "allowed requests is equal",
        actual.getAllowedRequests(),
        equalTo(lease.getAllowedRequests()));
    assertThat("timetolive is equal", actual.getTtl(), equalTo(lease.getTtl()));
    assertThat(
        "starting allowed request is equal",
        actual.getStartingAllowedRequests(),
        equalTo(lease.getStartingAllowedRequests()));
  }

  @Test
  public void leaseGrantUsed() throws Exception {
    leaseManager.leaseGranted(lease);
    leaseManager.useLease();
    LeaseImpl actualLease = leaseManager.getLease();
    assertThat(
        "allowed requests is equal",
        actualLease.getAllowedRequests(),
        equalTo(allowedRequests - 1));
    assertThat("timetolive is equal", actualLease.getTtl(), equalTo(ttl));
    assertThat(
        "starting allowed request is equal",
        actualLease.getStartingAllowedRequests(),
        equalTo(allowedRequests));
  }

  @Test
  public void leaseRequestsDoNotGoBelowZero() throws Exception {
    leaseManager.leaseGranted(0, 0, null);
    leaseManager.useLease();
    LeaseImpl actual = leaseManager.getLease();
    assertThat("lease requests do not go below zero", actual.getAllowedRequests(), equalTo(0));
  }
}
