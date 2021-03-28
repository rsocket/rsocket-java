package io.rsocket.core;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.LeaseFrameCodec;
import io.rsocket.test.TestDuplexConnection;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLLLL_Result;

public abstract class SlowFireAndForgetRequesterMonoStressTest {

  abstract static class BaseStressTest {

    final StressSubscriber<ByteBuf> outboundSubscriber = new StressSubscriber<>();

    final StressSubscriber<Void> stressSubscriber = new StressSubscriber<>();

    final TestDuplexConnection testDuplexConnection =
        new TestDuplexConnection(this.outboundSubscriber, false);

    final RequesterLeaseTracker requesterLeaseTracker =
        new RequesterLeaseTracker("test", maximumAllowedAwaitingPermitHandlersNumber());

    final TestRequesterResponderSupport requesterResponderSupport =
        new TestRequesterResponderSupport(
            testDuplexConnection, StreamIdSupplier.clientSupplier(), requesterLeaseTracker);

    final SlowFireAndForgetRequesterMono source = source();

    abstract SlowFireAndForgetRequesterMono source();

    abstract int maximumAllowedAwaitingPermitHandlersNumber();
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 3, 1, 0, 0"},
      expect = ACCEPTABLE)
  @State
  public static class TwoSubscribesRaceStressTest extends BaseStressTest {

    final StressSubscriber<Void> stressSubscriber1 = new StressSubscriber<>();

    @Override
    SlowFireAndForgetRequesterMono source() {
      return new SlowFireAndForgetRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    @Override
    int maximumAllowedAwaitingPermitHandlersNumber() {
      return 0;
    }

    // init
    {
      final ByteBuf leaseFrame =
          LeaseFrameCodec.encode(this.testDuplexConnection.alloc(), 1000, 1, null);
      this.requesterLeaseTracker.handleLeaseFrame(leaseFrame);
      leaseFrame.release();
    }

    @Actor
    public void subscribe1() {
      this.source.subscribe(this.stressSubscriber);
    }

    @Actor
    public void subscribe2() {
      this.source.subscribe(this.stressSubscriber1);
    }

    @Arbiter
    public void arbiter(LLLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 =
          this.stressSubscriber.onCompleteCalls
              + this.stressSubscriber.onErrorCalls * 2
              + this.stressSubscriber1.onCompleteCalls
              + this.stressSubscriber1.onErrorCalls * 2;
      r.r3 = this.outboundSubscriber.onNextCalls;
      r.r4 = this.requesterLeaseTracker.availableRequests;
      r.r5 = this.source.payload.refCnt();

      this.outboundSubscriber.values.forEach(ByteBuf::release);
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 1, 1, 0, 0"},
      expect = ACCEPTABLE,
      desc = "frame delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 1, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened first")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 0, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened in between")
  @State
  public static class SubscribeAndCancelRaceStressTest extends BaseStressTest {

    @Override
    SlowFireAndForgetRequesterMono source() {
      return new SlowFireAndForgetRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    @Override
    int maximumAllowedAwaitingPermitHandlersNumber() {
      return 0;
    }

    // init
    {
      final ByteBuf leaseFrame =
          LeaseFrameCodec.encode(this.testDuplexConnection.alloc(), 1000, 1, null);
      this.requesterLeaseTracker.handleLeaseFrame(leaseFrame);
      leaseFrame.release();
    }

    @Actor
    public void subscribe() {
      this.source.subscribe(this.stressSubscriber);
    }

    @Actor
    public void cancel() {
      this.stressSubscriber.cancel();
    }

    @Arbiter
    public void arbiter(LLLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 = this.stressSubscriber.onCompleteCalls + this.stressSubscriber.onErrorCalls * 2;
      r.r3 = this.outboundSubscriber.onNextCalls;
      r.r4 = this.requesterLeaseTracker.availableRequests;
      r.r5 = this.source.payload.refCnt();

      this.outboundSubscriber.values.forEach(ByteBuf::release);
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 1, 1, 0, 0"},
      expect = ACCEPTABLE,
      desc = "frame delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 0, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened in between")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 1, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened first")
  @State
  public static class SubscribeAndCancelWithDeferredLeaseRaceStressTest extends BaseStressTest {

    final ByteBuf leaseFrame =
        LeaseFrameCodec.encode(this.testDuplexConnection.alloc(), 1000, 1, null);

    @Override
    SlowFireAndForgetRequesterMono source() {
      return new SlowFireAndForgetRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    @Override
    int maximumAllowedAwaitingPermitHandlersNumber() {
      return 1;
    }

    @Actor
    public void issueLease() {
      final ByteBuf leaseFrame = this.leaseFrame;
      this.requesterLeaseTracker.handleLeaseFrame(leaseFrame);
      leaseFrame.release();
    }

    @Actor
    public void subscribe() {
      this.source.subscribe(this.stressSubscriber);
    }

    @Actor
    public void cancel() {
      this.stressSubscriber.cancel();
    }

    @Arbiter
    public void arbiter(LLLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 = this.stressSubscriber.onCompleteCalls + this.stressSubscriber.onErrorCalls * 2;
      r.r3 = this.outboundSubscriber.onNextCalls;
      r.r4 = this.requesterLeaseTracker.availableRequests;
      r.r5 = this.source.payload.refCnt();

      this.outboundSubscriber.values.forEach(ByteBuf::release);
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 1, 1, 0, 0"},
      expect = ACCEPTABLE,
      desc = "frame delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 2, 0, 1, 0"},
      expect = ACCEPTABLE,
      desc = "no lease error delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 1, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened first")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 0, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened in between")
  @Outcome(
      id = {"-9223372036854775808, 3, 0, 1, 0"},
      expect = ACCEPTABLE,
      desc =
          "cancellation happened after lease permit requested but before it was actually decided and in the case when no lease are available. Error is dropped")
  @State
  public static class SubscribeAndCancelWithDeferredLease2RaceStressTest extends BaseStressTest {

    final ByteBuf leaseFrame =
        LeaseFrameCodec.encode(this.testDuplexConnection.alloc(), 1000, 1, null);

    @Override
    SlowFireAndForgetRequesterMono source() {
      return new SlowFireAndForgetRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    @Override
    int maximumAllowedAwaitingPermitHandlersNumber() {
      return 0;
    }

    @Actor
    public void issueLease() {
      final ByteBuf leaseFrame = this.leaseFrame;
      this.requesterLeaseTracker.handleLeaseFrame(leaseFrame);
      leaseFrame.release();
    }

    @Actor
    public void subscribe() {
      this.source.subscribe(this.stressSubscriber);
    }

    @Actor
    public void cancel() {
      this.stressSubscriber.cancel();
    }

    @Arbiter
    public void arbiter(LLLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 =
          this.stressSubscriber.onCompleteCalls
              + this.stressSubscriber.onErrorCalls * 2
              + this.stressSubscriber.droppedErrors.size() * 3;
      r.r3 = this.outboundSubscriber.onNextCalls;
      r.r4 = this.requesterLeaseTracker.availableRequests;
      r.r5 = this.source.payload.refCnt();

      this.outboundSubscriber.values.forEach(ByteBuf::release);
    }
  }
}
