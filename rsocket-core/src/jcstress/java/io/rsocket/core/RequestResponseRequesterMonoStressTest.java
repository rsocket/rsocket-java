package io.rsocket.core;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.rsocket.Payload;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.LeaseFrameCodec;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.test.TestDuplexConnection;
import java.util.stream.IntStream;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLLLLL_Result;
import org.openjdk.jcstress.infra.results.LLLLL_Result;
import org.openjdk.jcstress.infra.results.LLLL_Result;

public abstract class RequestResponseRequesterMonoStressTest {

  abstract static class BaseStressTest {

    final StressSubscriber<ByteBuf> outboundSubscriber = new StressSubscriber<>();

    final StressSubscriber<Payload> stressSubscriber = new StressSubscriber<>(initialRequest());

    final TestDuplexConnection testDuplexConnection =
        new TestDuplexConnection(this.outboundSubscriber, false);

    final RequesterLeaseTracker requesterLeaseTracker;

    final TestRequesterResponderSupport requesterResponderSupport;

    final RequestResponseRequesterMono source;

    BaseStressTest(RequesterLeaseTracker requesterLeaseTracker) {
      this.requesterLeaseTracker = requesterLeaseTracker;
      this.requesterResponderSupport =
          new TestRequesterResponderSupport(
              testDuplexConnection, StreamIdSupplier.clientSupplier(), requesterLeaseTracker);
      this.source = source();
    }

    abstract RequestResponseRequesterMono source();

    abstract long initialRequest();
  }

  abstract static class BaseStressTestWithLease extends BaseStressTest {

    BaseStressTestWithLease(int maximumAllowedAwaitingPermitHandlersNumber) {
      super(new RequesterLeaseTracker("test", maximumAllowedAwaitingPermitHandlersNumber));
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 3, 1, 0, 0"},
      expect = ACCEPTABLE)
  @State
  public static class TwoSubscribesRaceStressTest extends BaseStressTestWithLease {

    final StressSubscriber<Payload> stressSubscriber1 = new StressSubscriber<>();

    public TwoSubscribesRaceStressTest() {
      super(0);
    }

    @Override
    RequestResponseRequesterMono source() {
      return new RequestResponseRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    @Override
    long initialRequest() {
      return Long.MAX_VALUE;
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
      final ByteBuf nextFrame =
          PayloadFrameCodec.encode(
              this.testDuplexConnection.alloc(),
              1,
              false,
              true,
              true,
              null,
              ByteBufUtil.writeUtf8(this.testDuplexConnection.alloc(), "response-data"));
      this.source.handleNext(nextFrame, false, true);
      nextFrame.release();

      r.r1 = this.source.state;
      r.r2 =
          this.stressSubscriber.onCompleteCalls
              + this.stressSubscriber.onErrorCalls * 2
              + this.stressSubscriber1.onCompleteCalls
              + this.stressSubscriber1.onErrorCalls * 2;
      r.r3 = this.outboundSubscriber.onNextCalls;
      r.r4 = this.requesterLeaseTracker.availableRequests;

      this.outboundSubscriber.values.forEach(ByteBuf::release);
      this.stressSubscriber.values.forEach(Payload::release);
      this.stressSubscriber1.values.forEach(Payload::release);

      r.r5 = this.source.payload.refCnt() + nextFrame.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 0, 2, 0, 0, " + (0x04 + 2 * 0x09)},
      expect = ACCEPTABLE,
      desc = "frame delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 1, 0, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened first")
  @State
  public static class SubscribeAndRequestAndCancelRaceStressTest extends BaseStressTestWithLease {

    public SubscribeAndRequestAndCancelRaceStressTest() {
      super(0);
    }

    @Override
    RequestResponseRequesterMono source() {
      return new RequestResponseRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    @Override
    long initialRequest() {
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

    @Actor
    public void request() {
      this.stressSubscriber.request(1);
      this.stressSubscriber.request(Long.MAX_VALUE);
      this.stressSubscriber.request(1);
    }

    @Arbiter
    public void arbiter(LLLLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 = this.stressSubscriber.onCompleteCalls + this.stressSubscriber.onErrorCalls * 2;
      r.r3 = this.outboundSubscriber.onNextCalls;
      r.r4 = this.requesterLeaseTracker.availableRequests;
      r.r5 = this.source.payload.refCnt();

      r.r6 =
          IntStream.range(0, this.outboundSubscriber.values.size())
              .map(
                  i ->
                      FrameHeaderCodec.frameType(this.outboundSubscriber.values.get(i))
                              .getEncodedType()
                          * (i + 1))
              .sum();

      this.outboundSubscriber.values.forEach(ByteBuf::release);
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 0, 2, 0, 0, " + (0x04 + 2 * 0x09)},
      expect = ACCEPTABLE,
      desc = "frame delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 1, 0, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened first")
  @State
  public static class SubscribeAndRequestAndCancelWithDeferredLeaseRaceStressTest
      extends BaseStressTestWithLease {

    final ByteBuf leaseFrame =
        LeaseFrameCodec.encode(this.testDuplexConnection.alloc(), 1000, 1, null);

    public SubscribeAndRequestAndCancelWithDeferredLeaseRaceStressTest() {
      super(1);
    }

    @Override
    RequestResponseRequesterMono source() {
      return new RequestResponseRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    @Override
    long initialRequest() {
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

    @Actor
    public void request() {
      this.stressSubscriber.request(1);
      this.stressSubscriber.request(Long.MAX_VALUE);
      this.stressSubscriber.request(1);
    }

    @Arbiter
    public void arbiter(LLLLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 = this.stressSubscriber.onCompleteCalls + this.stressSubscriber.onErrorCalls * 2;
      r.r3 = this.outboundSubscriber.onNextCalls;
      r.r4 = this.requesterLeaseTracker.availableRequests;
      r.r5 = this.source.payload.refCnt();
      r.r6 =
          IntStream.range(0, this.outboundSubscriber.values.size())
              .map(
                  i ->
                      FrameHeaderCodec.frameType(this.outboundSubscriber.values.get(i))
                              .getEncodedType()
                          * (i + 1))
              .sum();

      this.outboundSubscriber.values.forEach(ByteBuf::release);
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 0, 2, 0, 0, " + (0x04 + 2 * 0x09)},
      expect = ACCEPTABLE,
      desc = "frame delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 2, 0, 1, 0, 0"},
      expect = ACCEPTABLE,
      desc = "NoLeaseError delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 1, 0, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened first or in between")
  @Outcome(
      id = {"-9223372036854775808, 3, 0, 1, 0, 0"},
      expect = ACCEPTABLE,
      desc =
          "cancellation happened after lease permit requested but before it was actually decided and in the case when no lease are available. Error is dropped")
  @State
  public static class SubscribeAndRequestAndCancelWithDeferredLease2RaceStressTest
      extends BaseStressTestWithLease {

    final ByteBuf leaseFrame =
        LeaseFrameCodec.encode(this.testDuplexConnection.alloc(), 1000, 1, null);

    SubscribeAndRequestAndCancelWithDeferredLease2RaceStressTest() {
      super(0);
    }

    @Override
    RequestResponseRequesterMono source() {
      return new RequestResponseRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    @Override
    long initialRequest() {
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

    @Actor
    public void request() {
      this.stressSubscriber.request(1);
      this.stressSubscriber.request(Long.MAX_VALUE);
      this.stressSubscriber.request(1);
    }

    @Arbiter
    public void arbiter(LLLLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 =
          this.stressSubscriber.onCompleteCalls
              + this.stressSubscriber.onErrorCalls * 2
              + this.stressSubscriber.droppedErrors.size() * 3;
      r.r3 = this.outboundSubscriber.onNextCalls;
      r.r4 = this.requesterLeaseTracker.availableRequests;
      r.r5 = this.source.payload.refCnt();
      r.r6 =
          IntStream.range(0, this.outboundSubscriber.values.size())
              .map(
                  i ->
                      FrameHeaderCodec.frameType(this.outboundSubscriber.values.get(i))
                              .getEncodedType()
                          * (i + 1))
              .sum();

      this.outboundSubscriber.values.forEach(ByteBuf::release);
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 0, 2, 0, " + (0x04 + 2 * 0x09)},
      expect = ACCEPTABLE,
      desc = "frame delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 0, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened first")
  @State
  public static class SubscribeAndRequestAndCancel extends BaseStressTest {

    SubscribeAndRequestAndCancel() {
      super(null);
    }

    @Override
    RequestResponseRequesterMono source() {
      return new RequestResponseRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    @Override
    long initialRequest() {
      return 0;
    }

    @Actor
    public void subscribe() {
      this.source.subscribe(this.stressSubscriber);
    }

    @Actor
    public void cancel() {
      this.stressSubscriber.cancel();
    }

    @Actor
    public void request() {
      this.stressSubscriber.request(1);
      this.stressSubscriber.request(Long.MAX_VALUE);
      this.stressSubscriber.request(1);
    }

    @Arbiter
    public void arbiter(LLLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 =
          this.stressSubscriber.onCompleteCalls
              + this.stressSubscriber.onErrorCalls * 2
              + this.stressSubscriber.droppedErrors.size() * 3;
      r.r3 = this.outboundSubscriber.onNextCalls;
      r.r4 = this.source.payload.refCnt();
      r.r5 =
          IntStream.range(0, this.outboundSubscriber.values.size())
              .map(
                  i ->
                      FrameHeaderCodec.frameType(this.outboundSubscriber.values.get(i))
                              .getEncodedType()
                          * (i + 1))
              .sum();

      this.outboundSubscriber.values.forEach(ByteBuf::release);
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 1, 1, 0"},
      expect = ACCEPTABLE,
      desc = "frame delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened first or in between")
  @State
  public static class CancelWithInboundNextRaceStressTest extends BaseStressTestWithLease {

    final ByteBuf nextFrame =
        PayloadFrameCodec.encode(
            this.testDuplexConnection.alloc(),
            1,
            false,
            true,
            true,
            null,
            ByteBufUtil.writeUtf8(this.testDuplexConnection.alloc(), "response-data"));

    CancelWithInboundNextRaceStressTest() {
      super(0);
    }

    @Override
    RequestResponseRequesterMono source() {
      return new RequestResponseRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    // init
    {
      final ByteBuf leaseFrame =
          LeaseFrameCodec.encode(this.testDuplexConnection.alloc(), 1000, 1, null);
      this.requesterLeaseTracker.handleLeaseFrame(leaseFrame);
      leaseFrame.release();

      this.source.subscribe(this.stressSubscriber);
    }

    @Override
    long initialRequest() {
      return 1;
    }

    @Actor
    public void inboundNext() {
      this.source.handleNext(this.nextFrame, false, true);
      this.nextFrame.release();
    }

    @Actor
    public void cancel() {
      this.stressSubscriber.cancel();
    }

    @Arbiter
    public void arbiter(LLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 =
          this.stressSubscriber.onCompleteCalls
              + this.stressSubscriber.onErrorCalls * 2
              + this.stressSubscriber.droppedErrors.size() * 3;
      r.r3 = this.stressSubscriber.onNextCalls;

      this.outboundSubscriber.values.forEach(ByteBuf::release);
      this.stressSubscriber.values.forEach(Payload::release);

      r.r4 = this.source.payload.refCnt() + this.nextFrame.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 1, 0, 0"},
      expect = ACCEPTABLE,
      desc = "frame delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 0, 0, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened first or in between")
  @State
  public static class CancelWithInboundCompleteRaceStressTest extends BaseStressTestWithLease {

    CancelWithInboundCompleteRaceStressTest() {
      super(0);
    }

    @Override
    RequestResponseRequesterMono source() {
      return new RequestResponseRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    // init
    {
      final ByteBuf leaseFrame =
          LeaseFrameCodec.encode(this.testDuplexConnection.alloc(), 1000, 1, null);
      this.requesterLeaseTracker.handleLeaseFrame(leaseFrame);
      leaseFrame.release();

      this.source.subscribe(this.stressSubscriber);
    }

    @Override
    long initialRequest() {
      return 1;
    }

    @Actor
    public void inboundComplete() {
      this.source.handleComplete();
    }

    @Actor
    public void cancel() {
      this.stressSubscriber.cancel();
    }

    @Arbiter
    public void arbiter(LLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 =
          this.stressSubscriber.onCompleteCalls
              + this.stressSubscriber.onErrorCalls * 2
              + this.stressSubscriber.droppedErrors.size() * 3;
      r.r3 = this.stressSubscriber.onNextCalls;

      this.outboundSubscriber.values.forEach(ByteBuf::release);
      this.stressSubscriber.values.forEach(Payload::release);

      r.r4 = this.source.payload.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 2, 0, 0"},
      expect = ACCEPTABLE,
      desc = "frame delivered before cancellation")
  @Outcome(
      id = {"-9223372036854775808, 3, 0, 0"},
      expect = ACCEPTABLE,
      desc = "cancellation happened first. inbound error dropped")
  @State
  public static class CancelWithInboundErrorRaceStressTest extends BaseStressTestWithLease {

    static final RuntimeException ERROR = new RuntimeException("Test");

    CancelWithInboundErrorRaceStressTest() {
      super(0);
    }

    @Override
    RequestResponseRequesterMono source() {
      return new RequestResponseRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
    }

    // init
    {
      final ByteBuf leaseFrame =
          LeaseFrameCodec.encode(this.testDuplexConnection.alloc(), 1000, 1, null);
      this.requesterLeaseTracker.handleLeaseFrame(leaseFrame);
      leaseFrame.release();

      this.source.subscribe(this.stressSubscriber);
    }

    @Override
    long initialRequest() {
      return 1;
    }

    @Actor
    public void inboundError() {
      this.source.handleError(ERROR);
    }

    @Actor
    public void cancel() {
      this.stressSubscriber.cancel();
    }

    @Arbiter
    public void arbiter(LLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 =
          this.stressSubscriber.onCompleteCalls
              + this.stressSubscriber.onErrorCalls * 2
              + this.stressSubscriber.droppedErrors.size() * 3;
      r.r3 = this.stressSubscriber.onNextCalls;

      this.outboundSubscriber.values.forEach(ByteBuf::release);
      this.stressSubscriber.values.forEach(Payload::release);

      r.r4 = this.source.payload.refCnt();
    }
  }
}
