package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.util.concurrent.Queues;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class UpstreamFramesSubscribers implements Disposable {
  private final Collection<UpstreamFramesSubscriber> subs = new ArrayList<>();
  private final AtomicBoolean disposed = new AtomicBoolean();

  private final Queue<ByteBuf> framesCache;
  private final Consumer<ByteBuf> frameConsumer;
  private final int limitRate;

  public UpstreamFramesSubscribers(int limitRate, Consumer<ByteBuf> frameConsumer) {
    this.limitRate = limitRate;
    this.framesCache = Queues.<ByteBuf>unbounded(limitRate * 2).get();
    this.frameConsumer = frameConsumer;
  }

  public Subscriber<ByteBuf> create(Consumer<ByteBuf> frameConsumer) {
    UpstreamFramesSubscriber sub = new UpstreamFramesSubscriber(limitRate, frameConsumer, framesCache);
    subs.add(sub);
    return sub;
  }

  public void resumeStart() {
    subs.forEach(UpstreamFramesSubscriber::resumeStart);
  }

  public void resumeComplete() {
    ByteBuf frame = framesCache.poll();
    while (frame != null) {
      frameConsumer.accept(frame);
      frame = framesCache.poll();
    }
    subs.forEach(UpstreamFramesSubscriber::resumeComplete);
  }

  @Override
  public void dispose() {
    if (disposed.compareAndSet(false, true)) {
      subs.forEach(UpstreamFramesSubscriber::dispose);
      subs.clear();
      releaseCache();
    }
  }

  @Override
  public boolean isDisposed() {
    return disposed.get();
  }

  private void releaseCache() {
    ByteBuf frame = framesCache.poll();
    while (frame != null && frame.refCnt() > 0) {
      frame.release(frame.refCnt());
    }
  }
}
