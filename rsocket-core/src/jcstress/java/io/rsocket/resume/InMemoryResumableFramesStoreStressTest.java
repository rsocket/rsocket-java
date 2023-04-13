package io.rsocket.resume;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.internal.UnboundedProcessor;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LL_Result;
import reactor.core.Disposable;

public class InMemoryResumableFramesStoreStressTest {
  boolean storeClosed;

  InMemoryResumableFramesStore store =
      new InMemoryResumableFramesStore("test", Unpooled.EMPTY_BUFFER, 128);
  boolean processorClosed;
  UnboundedProcessor processor = new UnboundedProcessor(() -> processorClosed = true);

  void subscribe() {
    store.saveFrames(processor).subscribe();
    store.onClose().subscribe(null, t -> storeClosed = true, () -> storeClosed = true);
  }

  @JCStressTest
  @Outcome(
      id = {"true, true"},
      expect = ACCEPTABLE)
  @State
  public static class TwoSubscribesRaceStressTest extends InMemoryResumableFramesStoreStressTest {

    Disposable d1;

    final ByteBuf b1 =
        PayloadFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            1,
            false,
            true,
            false,
            ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "hello1"),
            ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "hello2"));
    final ByteBuf b2 =
        PayloadFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            3,
            false,
            true,
            false,
            ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "hello3"),
            ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "hello4"));
    final ByteBuf b3 =
        PayloadFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            5,
            false,
            true,
            false,
            ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "hello5"),
            ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "hello6"));

    final ByteBuf c1 =
        ErrorFrameCodec.encode(ByteBufAllocator.DEFAULT, 0, new ConnectionErrorException("closed"));

    {
      subscribe();
      d1 = store.doOnDiscard(ByteBuf.class, ByteBuf::release).subscribe(ByteBuf::release, t -> {});
    }

    @Actor
    public void producer1() {
      processor.tryEmitNormal(b1);
      processor.tryEmitNormal(b2);
      processor.tryEmitNormal(b3);
    }

    @Actor
    public void producer2() {
      processor.tryEmitFinal(c1);
    }

    @Actor
    public void producer3() {
      d1.dispose();
      store
          .doOnDiscard(ByteBuf.class, ByteBuf::release)
          .subscribe(ByteBuf::release, t -> {})
          .dispose();
      store
          .doOnDiscard(ByteBuf.class, ByteBuf::release)
          .subscribe(ByteBuf::release, t -> {})
          .dispose();
      store.doOnDiscard(ByteBuf.class, ByteBuf::release).subscribe(ByteBuf::release, t -> {});
    }

    @Actor
    public void producer4() {
      store.releaseFrames(0);
      store.releaseFrames(0);
      store.releaseFrames(0);
    }

    @Arbiter
    public void arbiter(LL_Result r) {
      r.r1 = storeClosed;
      r.r2 = processorClosed;
    }
  }
}
