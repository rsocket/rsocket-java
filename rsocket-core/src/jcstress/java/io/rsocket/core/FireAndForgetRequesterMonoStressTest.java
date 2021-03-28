package io.rsocket.core;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

import io.netty.buffer.ByteBuf;
import io.rsocket.test.TestDuplexConnection;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLLL_Result;

public abstract class FireAndForgetRequesterMonoStressTest {

  abstract static class BaseStressTest {

    final StressSubscriber<ByteBuf> outboundSubscriber = new StressSubscriber<>();

    final StressSubscriber<Void> stressSubscriber = new StressSubscriber<>();

    final TestDuplexConnection testDuplexConnection =
        new TestDuplexConnection(this.outboundSubscriber, false);

    final TestRequesterResponderSupport requesterResponderSupport =
        new TestRequesterResponderSupport(testDuplexConnection, StreamIdSupplier.clientSupplier());

    final FireAndForgetRequesterMono source = source();

    abstract FireAndForgetRequesterMono source();
  }

  @JCStressTest
  @Outcome(
      id = {"-9223372036854775808, 3, 1, 0"},
      expect = ACCEPTABLE)
  @State
  public static class TwoSubscribesRaceStressTest extends BaseStressTest {

    final StressSubscriber<Void> stressSubscriber1 = new StressSubscriber<>();

    @Override
    FireAndForgetRequesterMono source() {
      return new FireAndForgetRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
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
    public void arbiter(LLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 =
          this.stressSubscriber.onCompleteCalls
              + this.stressSubscriber.onErrorCalls * 2
              + this.stressSubscriber1.onCompleteCalls
              + this.stressSubscriber1.onErrorCalls * 2;
      r.r3 = this.outboundSubscriber.onNextCalls;
      r.r4 = this.source.payload.refCnt();

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
      desc = "cancellation happened first")
  @State
  public static class SubscribeAndCancelRaceStressTest extends BaseStressTest {

    @Override
    FireAndForgetRequesterMono source() {
      return new FireAndForgetRequesterMono(
          UnpooledByteBufPayload.create(
              "test-data", "test-metadata", this.requesterResponderSupport.getAllocator()),
          this.requesterResponderSupport);
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
    public void arbiter(LLLL_Result r) {
      r.r1 = this.source.state;
      r.r2 = this.stressSubscriber.onCompleteCalls + this.stressSubscriber.onErrorCalls * 2;
      r.r3 = this.outboundSubscriber.onNextCalls;
      r.r4 = this.source.payload.refCnt();

      this.outboundSubscriber.values.forEach(ByteBuf::release);
    }
  }
}
