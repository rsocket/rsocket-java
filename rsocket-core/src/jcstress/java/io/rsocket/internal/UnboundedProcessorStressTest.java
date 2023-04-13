package io.rsocket.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.rsocket.core.StressSubscriber;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLLL_Result;
import org.openjdk.jcstress.infra.results.LLL_Result;
import org.openjdk.jcstress.infra.results.L_Result;
import reactor.core.Fuseable;
import reactor.core.publisher.Hooks;

public abstract class UnboundedProcessorStressTest {

  static {
    Hooks.onErrorDropped(t -> {});
  }

  final UnboundedProcessor unboundedProcessor = new UnboundedProcessor();

  @JCStressTest
  @Outcome(
      id = {
        "0, 1, 0",
        "1, 1, 0",
        "2, 1, 0",
        "3, 1, 0",
        "4, 1, 0",

        // dropped error scenarios
        "0, 4, 0",
        "1, 4, 0",
        "2, 4, 0",
        "3, 4, 0",
        "4, 4, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onComplete() before dispose() || onError()")
  @Outcome(
      id = {
        "0, 2, 0", "1, 2, 0", "2, 2, 0", "3, 2, 0", "4, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onError() before dispose() || onComplete()")
  @Outcome(
      id = {
        "0, 2, 0",
        "1, 2, 0",
        "2, 2, 0",
        "3, 2, 0",
        "4, 2, 0",
        // dropped error
        "0, 5, 0",
        "1, 5, 0",
        "2, 5, 0",
        "3, 5, 0",
        "4, 5, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before onError() || onComplete()")
  @Outcome(
      id = {
        "0, 0, 0",
        "1, 0, 0",
        "2, 0, 0",
        "3, 0, 0",
        "4, 0, 0",
        // interleave with error or complete happened first but dispose suppressed them
        "0, 3, 0",
        "1, 3, 0",
        "2, 3, 0",
        "3, 3, 0",
        "4, 3, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "cancel() before or interleave with dispose() || onError() || onComplete()")
  @State
  public static class SmokeStressTest extends UnboundedProcessorStressTest {

    static final RuntimeException testException = new RuntimeException("test");

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.NONE);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    {
      unboundedProcessor.subscribe(stressSubscriber);
    }

    @Actor
    public void request() {
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void cancel() {
      stressSubscriber.cancel();
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Actor
    public void complete() {
      unboundedProcessor.onComplete();
    }

    @Actor
    public void error() {
      unboundedProcessor.onError(testException);
    }

    @Arbiter
    public void arbiter(LLL_Result r) {
      r.r1 = stressSubscriber.onNextCalls;
      r.r2 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r3 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "0, 1, 0",
        "1, 1, 0",
        "2, 1, 0",
        "3, 1, 0",
        "4, 1, 0",

        // dropped error scenarios
        "0, 4, 0",
        "1, 4, 0",
        "2, 4, 0",
        "3, 4, 0",
        "4, 4, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onComplete() before dispose() || onError()")
  @Outcome(
      id = {
        "0, 2, 0", "1, 2, 0", "2, 2, 0", "3, 2, 0", "4, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onError() before dispose() || onComplete()")
  @Outcome(
      id = {
        "0, 2, 0",
        "1, 2, 0",
        "2, 2, 0",
        "3, 2, 0",
        "4, 2, 0",
        // dropped error
        "0, 5, 0",
        "1, 5, 0",
        "2, 5, 0",
        "3, 5, 0",
        "4, 5, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before onError() || onComplete()")
  @Outcome(
      id = {
        "0, 0, 0",
        "1, 0, 0",
        "2, 0, 0",
        "3, 0, 0",
        "4, 0, 0",
        // interleave with error or complete happened first but dispose suppressed them
        "0, 3, 0",
        "1, 3, 0",
        "2, 3, 0",
        "3, 3, 0",
        "4, 3, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "cancel() before or interleave with dispose() || onError() || onComplete()")
  @State
  public static class SmokeFusedStressTest extends UnboundedProcessorStressTest {

    static final RuntimeException testException = new RuntimeException("test");

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.ANY);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    {
      unboundedProcessor.subscribe(stressSubscriber);
    }

    @Actor
    public void request() {
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void cancel() {
      stressSubscriber.cancel();
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Actor
    public void complete() {
      unboundedProcessor.onComplete();
    }

    @Actor
    public void error() {
      unboundedProcessor.onError(testException);
    }

    @Arbiter
    public void arbiter(LLL_Result r) {
      r.r1 = stressSubscriber.onNextCalls;
      r.r2 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r3 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "0, 1, 0",
        "1, 1, 0",
        "2, 1, 0",
        "3, 1, 0",
        "4, 1, 0",

        // dropped error scenarios
        "0, 4, 0",
        "1, 4, 0",
        "2, 4, 0",
        "3, 4, 0",
        "4, 4, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onComplete() before dispose() || onError()")
  @Outcome(
      id = {
        "0, 2, 0", "1, 2, 0", "2, 2, 0", "3, 2, 0", "4, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onError() before dispose() || onComplete()")
  @Outcome(
      id = {
        "0, 2, 0",
        "1, 2, 0",
        "2, 2, 0",
        "3, 2, 0",
        "4, 2, 0",
        // dropped error
        "0, 5, 0",
        "1, 5, 0",
        "2, 5, 0",
        "3, 5, 0",
        "4, 5, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before onError() || onComplete()")
  @State
  public static class Smoke2StressTest extends UnboundedProcessorStressTest {

    static final RuntimeException testException = new RuntimeException("test");

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.NONE);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    @Actor
    public void subscribeAndRequest() {
      unboundedProcessor.subscribe(stressSubscriber);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Actor
    public void complete() {
      unboundedProcessor.onComplete();
    }

    @Actor
    public void error() {
      unboundedProcessor.onError(testException);
    }

    @Arbiter
    public void arbiter(LLL_Result r) {
      r.r1 = stressSubscriber.onNextCalls;
      r.r2 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      if (stressSubscriber.onCompleteCalls > 0 && stressSubscriber.onErrorCalls > 0) {
        throw new RuntimeException("boom");
      }

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r3 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "0, 1, 0",
        "1, 1, 0",
        "2, 1, 0",
        "3, 1, 0",
        "4, 1, 0",

        // dropped error scenarios
        "0, 4, 0",
        "1, 4, 0",
        "2, 4, 0",
        "3, 4, 0",
        "4, 4, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onComplete() before dispose() || onError()")
  @Outcome(
      id = {
        "0, 2, 0", "1, 2, 0", "2, 2, 0", "3, 2, 0", "4, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onError() before dispose() || onComplete()")
  @Outcome(
      id = {
        "0, 2, 0",
        "1, 2, 0",
        "2, 2, 0",
        "3, 2, 0",
        "4, 2, 0",
        // dropped error
        "0, 5, 0",
        "1, 5, 0",
        "2, 5, 0",
        "3, 5, 0",
        "4, 5, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before onError() || onComplete()")
  @State
  public static class Smoke24StressTest extends UnboundedProcessorStressTest {

    static final RuntimeException testException = new RuntimeException("test");

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.NONE);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    @Actor
    public void subscribeAndRequest() {
      unboundedProcessor.subscribe(stressSubscriber);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Actor
    public void complete() {
      unboundedProcessor.onComplete();
    }

    @Actor
    public void error() {
      unboundedProcessor.onError(testException);
    }

    @Arbiter
    public void arbiter(LLL_Result r) {
      r.r1 = stressSubscriber.onNextCalls;
      r.r2 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r3 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "0, 1, 0",
        "1, 1, 0",
        "2, 1, 0",
        "3, 1, 0",
        "4, 1, 0",

        // dropped error scenarios
        "0, 4, 0",
        "1, 4, 0",
        "2, 4, 0",
        "3, 4, 0",
        "4, 4, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onComplete() before dispose() || onError()")
  @Outcome(
      id = {
        "0, 2, 0", "1, 2, 0", "2, 2, 0", "3, 2, 0", "4, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onError() before dispose() || onComplete()")
  @Outcome(
      id = {
        "0, 2, 0",
        "1, 2, 0",
        "2, 2, 0",
        "3, 2, 0",
        "4, 2, 0",
        // dropped error
        "0, 5, 0",
        "1, 5, 0",
        "2, 5, 0",
        "3, 5, 0",
        "4, 5, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before onError() || onComplete()")
  @State
  public static class Smoke2FusedStressTest extends UnboundedProcessorStressTest {

    static final RuntimeException testException = new RuntimeException("test");

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.ANY);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    @Actor
    public void subscribeAndRequest() {
      unboundedProcessor.subscribe(stressSubscriber);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Actor
    public void complete() {
      unboundedProcessor.onComplete();
    }

    @Actor
    public void error() {
      unboundedProcessor.onError(testException);
    }

    @Arbiter
    public void arbiter(LLL_Result r) {
      r.r1 = stressSubscriber.onNextCalls;
      r.r2 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r3 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "0, 1, 0",
        "1, 1, 0",
        "2, 1, 0",
        "3, 1, 0",
        "4, 1, 0",

        // dropped error scenarios
        "0, 4, 0",
        "1, 4, 0",
        "2, 4, 0",
        "3, 4, 0",
        "4, 4, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onComplete() before dispose() || onError()")
  @Outcome(
      id = {
        "0, 2, 0", "1, 2, 0", "2, 2, 0", "3, 2, 0", "4, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onError() before dispose() || onComplete()")
  @Outcome(
      id = {
        "0, 2, 0",
        "1, 2, 0",
        "2, 2, 0",
        "3, 2, 0",
        "4, 2, 0",
        // dropped error
        "0, 5, 0",
        "1, 5, 0",
        "2, 5, 0",
        "3, 5, 0",
        "4, 5, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before onError() || onComplete()")
  @Outcome(
      id = {
        "0, 0, 0",
        "1, 0, 0",
        "2, 0, 0",
        "3, 0, 0",
        "4, 0, 0",
        // interleave with error or complete happened first but dispose suppressed them
        "0, 3, 0",
        "1, 3, 0",
        "2, 3, 0",
        "3, 3, 0",
        "4, 3, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "cancel() before or interleave with dispose() || onError() || onComplete()")
  @State
  public static class Smoke21FusedStressTest extends UnboundedProcessorStressTest {

    static final RuntimeException testException = new RuntimeException("test");

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.ANY);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    @Actor
    public void subscribeAndRequest() {
      unboundedProcessor.subscribe(stressSubscriber);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void cancel() {
      stressSubscriber.cancel();
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Actor
    public void complete() {
      unboundedProcessor.onComplete();
    }

    @Actor
    public void error() {
      unboundedProcessor.onError(testException);
    }

    @Arbiter
    public void arbiter(LLL_Result r) {
      r.r1 = stressSubscriber.onNextCalls;
      r.r2 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r3 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "0, 1, 0", "1, 1, 0", "2, 1, 0", "3, 1, 0", "4, 1, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onComplete()")
  @Outcome(
      id = {
        "0, 0, 0",
        "1, 0, 0",
        "2, 0, 0",
        "3, 0, 0",
        "4, 0, 0",
        // interleave with error or complete happened first but dispose suppressed them
        "0, 3, 0",
        "1, 3, 0",
        "2, 3, 0",
        "3, 3, 0",
        "4, 3, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "cancel() before or interleave with onComplete()")
  @State
  public static class Smoke30StressTest extends UnboundedProcessorStressTest {

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.NONE);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    {
      unboundedProcessor.subscribe(stressSubscriber);
    }

    @Actor
    public void subscribeAndRequest() {
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void cancel() {
      stressSubscriber.cancel();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Actor
    public void complete() {
      unboundedProcessor.onComplete();
    }

    @Arbiter
    public void arbiter(LLL_Result r) {
      r.r1 = stressSubscriber.onNextCalls;
      r.r2 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r3 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "0, 1, 0", "1, 1, 0", "2, 1, 0", "3, 1, 0", "4, 1, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onComplete()")
  @State
  public static class Smoke31StressTest extends UnboundedProcessorStressTest {

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.NONE);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    {
      unboundedProcessor.subscribe(stressSubscriber);
    }

    @Actor
    public void subscribeAndRequest() {
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Actor
    public void complete() {
      unboundedProcessor.onComplete();
    }

    @Arbiter
    public void arbiter(LLL_Result r) {
      r.r1 = stressSubscriber.onNextCalls;
      r.r2 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r3 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "0, 1, 0", "1, 1, 0", "2, 1, 0", "3, 1, 0", "4, 1, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onComplete()")
  @State
  public static class Smoke32StressTest extends UnboundedProcessorStressTest {

    final StressSubscriber<ByteBuf> stressSubscriber =
        new StressSubscriber<>(Long.MAX_VALUE, Fuseable.NONE);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    {
      unboundedProcessor.subscribe(stressSubscriber);
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Actor
    public void complete() {
      unboundedProcessor.onComplete();
    }

    @Arbiter
    public void arbiter(LLL_Result r) {
      r.r1 = stressSubscriber.onNextCalls;
      r.r2 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r3 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "0, 1, 0, 5",
        "1, 1, 0, 5",
        "2, 1, 0, 5",
        "3, 1, 0, 5",
        "4, 1, 0, 5",
        "5, 1, 0, 5",
      },
      expect = Expect.ACCEPTABLE,
      desc = "onComplete()")
  @State
  public static class Smoke33StressTest extends UnboundedProcessorStressTest {

    final StressSubscriber<ByteBuf> stressSubscriber =
        new StressSubscriber<>(Long.MAX_VALUE, Fuseable.NONE);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);
    final ByteBuf byteBuf5 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(5);

    {
      unboundedProcessor.subscribe(stressSubscriber);
    }

    @Actor
    public void next1() {
      unboundedProcessor.tryEmitNormal(byteBuf1);
      unboundedProcessor.tryEmitPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.tryEmitPrioritized(byteBuf3);
      unboundedProcessor.tryEmitNormal(byteBuf4);
    }

    @Actor
    public void complete() {
      unboundedProcessor.tryEmitFinal(byteBuf5);
    }

    @Arbiter
    public void arbiter(LLLL_Result r) {
      r.r1 = stressSubscriber.onNextCalls;
      r.r2 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      r.r4 = stressSubscriber.values.get(stressSubscriber.values.size() - 1).readByte();
      stressSubscriber.values.forEach(ByteBuf::release);

      r.r3 =
          byteBuf1.refCnt()
              + byteBuf2.refCnt()
              + byteBuf3.refCnt()
              + byteBuf4.refCnt()
              + byteBuf5.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "-2954361355555045376, 4, 2, 0",
        "-3242591731706757120, 4, 2, 0",
        "-4107282860161892352, 4, 2, 0",
        "-4395513236313604096, 4, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 4, 0, 0", // here, dispose is earlier, but it was late to deliver
        // error signal in the drainLoop
        "-7566047373982433280, 4, 0, 0",
        "-7854277750134145024, 4, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 3, 2, 0",
        "-3242591731706757120, 3, 2, 0",
        "-4107282860161892352, 3, 2, 0",
        "-4395513236313604096, 3, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 3, 0, 0", // here, dispose is earlier, but it was late to deliver
        // error signal in the drainLoop
        "-7566047373982433280, 3, 0, 0",
        "-7854277750134145024, 3, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 2, 2, 0",
        "-3242591731706757120, 2, 2, 0",
        "-4107282860161892352, 2, 2, 0",
        "-4395513236313604096, 2, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 2, 0, 0", // here, dispose is earlier, but it was late to deliver
        // error signal in the drainLoop
        "-7566047373982433280, 2, 0, 0",
        "-7854277750134145024, 2, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 1, 2, 0",
        "-3242591731706757120, 1, 2, 0",
        "-4107282860161892352, 1, 2, 0",
        "-4395513236313604096, 1, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> dispose() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 1, 0, 0", // here, dispose is earlier, but it was late to deliver
        // error signal in the drainLoop
        "-7566047373982433280, 1, 0, 0",
        "-7854277750134145024, 1, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> cancel() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 0, 2, 0",
        "-3242591731706757120, 0, 2, 0",
        "-4107282860161892352, 0, 2, 0",
        "-4395513236313604096, 0, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 0, 0, 0", // here, dispose is earlier, but it was late to deliver
        // error signal in the drainLoop
        "-7566047373982433280, 0, 0, 0",
        "-7854277750134145024, 0, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> cancel() before anything")
  @State
  public static class RequestVsCancelVsOnNextVsDisposeStressTest
      extends UnboundedProcessorStressTest {

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.NONE);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    {
      unboundedProcessor.subscribe(stressSubscriber);
    }

    @Actor
    public void request() {
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void cancel() {
      stressSubscriber.cancel();
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Arbiter
    public void arbiter(LLLL_Result r) {
      r.r1 = unboundedProcessor.state;
      r.r2 = stressSubscriber.onNextCalls;
      r.r3 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r4 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "-3242591731706757120, 4, 2, 0",
        "-4107282860161892352, 4, 2, 0",
        "-4395513236313604096, 4, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-7854277750134145024, 4, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-3242591731706757120, 3, 2, 0",
        "-4107282860161892352, 3, 2, 0",
        "-4395513236313604096, 3, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-7854277750134145024, 3, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-3242591731706757120, 2, 2, 0",
        "-4107282860161892352, 2, 2, 0",
        "-4395513236313604096, 2, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-7854277750134145024, 2, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-3242591731706757120, 1, 2, 0",
        "-4107282860161892352, 1, 2, 0",
        "-4395513236313604096, 1, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> dispose() before anything")
  @Outcome(
      id = {
        "-7854277750134145024, 1, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> cancel() before anything")
  @Outcome(
      id = {
        "-3242591731706757120, 0, 2, 0",
        "-4107282860161892352, 0, 2, 0",
        "-4395513236313604096, 0, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before anything")
  @Outcome(
      id = {
        "-7854277750134145024, 0, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> cancel() before anything")
  @State
  public static class RequestVsCancelVsOnNextVsDisposeFusedStressTest
      extends UnboundedProcessorStressTest {

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.ANY);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    {
      unboundedProcessor.subscribe(stressSubscriber);
    }

    @Actor
    public void request() {
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void cancel() {
      stressSubscriber.cancel();
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Arbiter
    public void arbiter(LLLL_Result r) {
      r.r1 = unboundedProcessor.state;
      r.r2 = stressSubscriber.onNextCalls;
      r.r3 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r4 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "-2954361355555045376, 4, 2, 0",
        "-3242591731706757120, 4, 2, 0",
        "-4107282860161892352, 4, 2, 0",
        "-4395513236313604096, 4, 2, 0",
        "-4539628424389459968, 4, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 4, 0, 0", // here, dispose is earlier, but it was late to deliver
        // error signal in the drainLoop
        "-7566047373982433280, 4, 0, 0",
        "-7854277750134145024, 4, 0, 0",
        "-4539628424389459968, 4, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 3, 2, 0",
        "-3242591731706757120, 3, 2, 0",
        "-4107282860161892352, 3, 2, 0",
        "-4395513236313604096, 3, 2, 0",
        "-4539628424389459968, 3, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 3, 0, 0", // here, dispose is earlier, but it was late to deliver
        // error signal in the drainLoop
        "-7566047373982433280, 3, 0, 0",
        "-7854277750134145024, 3, 0, 0",
        "-4539628424389459968, 3, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 2, 2, 0",
        "-3242591731706757120, 2, 2, 0",
        "-4107282860161892352, 2, 2, 0",
        "-4395513236313604096, 2, 2, 0",
        "-4539628424389459968, 2, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 2, 0, 0", // here, dispose is earlier, but it was late to deliver
        // error signal in the drainLoop
        "-7566047373982433280, 2, 0, 0",
        "-7854277750134145024, 2, 0, 0",
        "-4539628424389459968, 2, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 1, 2, 0",
        "-3242591731706757120, 1, 2, 0",
        "-4107282860161892352, 1, 2, 0",
        "-4395513236313604096, 1, 2, 0",
        "-4539628424389459968, 1, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> dispose() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 1, 0, 0", // here, dispose is earlier, but it was late to deliver
        // error signal in the drainLoop
        "-7566047373982433280, 1, 0, 0",
        "-7854277750134145024, 1, 0, 0",
        "-4539628424389459968, 1, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> cancel() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 0, 2, 0",
        "-3242591731706757120, 0, 2, 0",
        "-4107282860161892352, 0, 2, 0",
        "-4395513236313604096, 0, 2, 0",
        "-4539628424389459968, 0, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before anything")
  @Outcome(
      id = {
        "-2954361355555045376, 0, 0, 0", // here, dispose is earlier, but it was late to deliver
        // error signal in the drainLoop
        "-7566047373982433280, 0, 0, 0",
        "-7854277750134145024, 0, 0, 0",
        "-4539628424389459968, 0, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> cancel() before anything")
  @State
  public static class SubscribeWithFollowingRequestsVsOnNextVsDisposeStressTest
      extends UnboundedProcessorStressTest {

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.NONE);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    @Actor
    public void subscribeAndRequest() {
      unboundedProcessor.subscribe(stressSubscriber);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Arbiter
    public void arbiter(LLLL_Result r) {
      r.r1 = unboundedProcessor.state;
      r.r2 = stressSubscriber.onNextCalls;
      r.r3 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r4 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {
        "-3242591731706757120, 4, 2, 0",
        "-4107282860161892352, 4, 2, 0",
        "-4395513236313604096, 4, 2, 0",
        "-4539628424389459968, 4, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-7854277750134145024, 4, 0, 0",
        "-4539628424389459968, 4, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-3242591731706757120, 3, 2, 0",
        "-4107282860161892352, 3, 2, 0",
        "-4395513236313604096, 3, 2, 0",
        "-4539628424389459968, 3, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-7854277750134145024, 3, 0, 0",
        "-4539628424389459968, 3, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-3242591731706757120, 2, 2, 0",
        "-4107282860161892352, 2, 2, 0",
        "-4395513236313604096, 2, 2, 0",
        "-4539628424389459968, 2, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> dispose() before anything")
  @Outcome(
      id = {
        "-7854277750134145024, 2, 0, 0",
        "-4539628424389459968, 2, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1, buf2) -> cancel() before anything")
  @Outcome(
      id = {
        "-3242591731706757120, 1, 2, 0",
        "-4107282860161892352, 1, 2, 0",
        "-4395513236313604096, 1, 2, 0",
        "-4539628424389459968, 1, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> dispose() before anything")
  @Outcome(
      id = {
        "-7854277750134145024, 1, 0, 0",
        "-4539628424389459968, 1, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> cancel() before anything")
  @Outcome(
      id = {
        "-3242591731706757120, 0, 2, 0",
        "-4107282860161892352, 0, 2, 0",
        "-4395513236313604096, 0, 2, 0",
        "-4539628424389459968, 0, 2, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before anything")
  @Outcome(
      id = {
        "-7854277750134145024, 0, 0, 0",
        "-4539628424389459968, 0, 0, 0",
      },
      expect = Expect.ACCEPTABLE,
      desc = "next(buf1) -> cancel() before anything")
  @State
  public static class SubscribeWithFollowingRequestsVsOnNextVsDisposeFusedStressTest
      extends UnboundedProcessorStressTest {

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.ANY);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    @Actor
    public void subscribeAndRequest() {
      unboundedProcessor.subscribe(stressSubscriber);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
      stressSubscriber.request(1);
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Arbiter
    public void arbiter(LLLL_Result r) {
      r.r1 = unboundedProcessor.state;
      r.r2 = stressSubscriber.onNextCalls;
      r.r3 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r4 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-4539628424389459968, 0, 2, 0", "-3386706919782612992, 0, 2, 0"},
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before anything")
  @Outcome(
      id = {"-4395513236313604096, 0, 2, 0"},
      expect = Expect.ACCEPTABLE,
      desc = "subscribe() -> dispose() before anything")
  @Outcome(
      id = {"-3242591731706757120, 0, 2, 0", "-3242591731706757120, 0, 0, 0"},
      expect = Expect.ACCEPTABLE,
      desc = "subscribe() -> (dispose() || cancel())")
  @Outcome(
      id = {"-7854277750134145024, 0, 0, 0"},
      expect = Expect.ACCEPTABLE,
      desc = "subscribe() -> cancel() before anything")
  @State
  public static class SubscribeWithFollowingCancelVsOnNextVsDisposeStressTest
      extends UnboundedProcessorStressTest {

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.NONE);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    @Actor
    public void subscribeAndCancel() {
      unboundedProcessor.subscribe(stressSubscriber);
      stressSubscriber.cancel();
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Arbiter
    public void arbiter(LLLL_Result r) {
      r.r1 = unboundedProcessor.state;
      r.r2 = stressSubscriber.onNextCalls;
      r.r3 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r4 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"-4539628424389459968, 0, 2, 0", "-3386706919782612992, 0, 2, 0"},
      expect = Expect.ACCEPTABLE,
      desc = "dispose() before anything")
  @Outcome(
      id = {"-4395513236313604096, 0, 2, 0"},
      expect = Expect.ACCEPTABLE,
      desc = "subscribe() -> dispose() before anything")
  @Outcome(
      id = {"-3242591731706757120, 0, 2, 0", "-3242591731706757120, 0, 0, 0"},
      expect = Expect.ACCEPTABLE,
      desc = "subscribe() -> (dispose() || cancel())")
  @Outcome(
      id = {"-7854277750134145024, 0, 0, 0"},
      expect = Expect.ACCEPTABLE,
      desc = "subscribe() -> cancel() before anything")
  @State
  public static class SubscribeWithFollowingCancelVsOnNextVsDisposeFusedStressTest
      extends UnboundedProcessorStressTest {

    final StressSubscriber<ByteBuf> stressSubscriber = new StressSubscriber<>(0, Fuseable.ANY);
    final ByteBuf byteBuf1 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(1);
    final ByteBuf byteBuf2 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(2);
    final ByteBuf byteBuf3 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(3);
    final ByteBuf byteBuf4 = UnpooledByteBufAllocator.DEFAULT.buffer().writeByte(4);

    @Actor
    public void subscribeAndCancel() {
      unboundedProcessor.subscribe(stressSubscriber);
      stressSubscriber.cancel();
    }

    @Actor
    public void dispose() {
      unboundedProcessor.dispose();
    }

    @Actor
    public void next1() {
      unboundedProcessor.onNext(byteBuf1);
      unboundedProcessor.onNextPrioritized(byteBuf2);
    }

    @Actor
    public void next2() {
      unboundedProcessor.onNextPrioritized(byteBuf3);
      unboundedProcessor.onNext(byteBuf4);
    }

    @Arbiter
    public void arbiter(LLLL_Result r) {
      r.r1 = unboundedProcessor.state;
      r.r2 = stressSubscriber.onNextCalls;
      r.r3 =
          stressSubscriber.onCompleteCalls
              + stressSubscriber.onErrorCalls * 2
              + stressSubscriber.droppedErrors.size() * 3;

      stressSubscriber.values.forEach(ByteBuf::release);

      r.r4 = byteBuf1.refCnt() + byteBuf2.refCnt() + byteBuf3.refCnt() + byteBuf4.refCnt();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"1"},
      expect = Expect.ACCEPTABLE)
  @State
  public static class SubscribeVsSubscribeStressTest extends UnboundedProcessorStressTest {

    final StressSubscriber<ByteBuf> stressSubscriber1 = new StressSubscriber<>(0, Fuseable.NONE);
    final StressSubscriber<ByteBuf> stressSubscriber2 = new StressSubscriber<>(0, Fuseable.NONE);

    @Actor
    public void subscribe1() {
      unboundedProcessor.subscribe(stressSubscriber1);
    }

    @Actor
    public void subscribe2() {
      unboundedProcessor.subscribe(stressSubscriber2);
    }

    @Arbiter
    public void arbiter(L_Result r) {
      r.r1 = stressSubscriber1.onErrorCalls + stressSubscriber2.onErrorCalls;
    }
  }
}
