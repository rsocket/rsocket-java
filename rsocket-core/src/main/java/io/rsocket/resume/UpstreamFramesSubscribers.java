package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class UpstreamFramesSubscribers implements Disposable {
  private static final Logger logger = LoggerFactory.getLogger(UpstreamFramesSubscribers.class);

  private final Collection<UpstreamFramesSubscriber> subs = new ArrayList<>();
  private final AtomicBoolean disposed = new AtomicBoolean();

  private final Queue<ByteBuf> framesCache;
  private final Consumer<ByteBuf> frameConsumer;
  private final Disposable downstreamRequestDisposable;
  private final Disposable resumeSaveStreamDisposable;
  private long downStreamRequestN;
  private long resumeSaveStreamRequestN;

  public UpstreamFramesSubscribers(
      Flux<Long> downstreamRequests,
      Flux<Long> resumeSaveStreamRequests,
      int estimatedDownstreamRequestN,
      int estimatedSubscribersCount,
      Consumer<ByteBuf> frameConsumer) {
    this.framesCache = Queues.<ByteBuf>unbounded(
        estimatedDownstreamRequestN * estimatedSubscribersCount).get();
    this.frameConsumer = frameConsumer;

    downstreamRequestDisposable =
        downstreamRequests
            .subscribe(requestN -> {
              downStreamRequestN = Operators.addCap(downStreamRequestN, requestN);
              requestN();
            });

    resumeSaveStreamDisposable =
        resumeSaveStreamRequests
            .subscribe(requestN -> {
              resumeSaveStreamRequestN = Operators.addCap(resumeSaveStreamRequestN, requestN);
              requestN();
            });
  }

  public Subscriber<ByteBuf> create(Consumer<ByteBuf> frameConsumer) {
    UpstreamFramesSubscriber sub = new UpstreamFramesSubscriber(
        frameConsumer,
        framesCache);
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
      downstreamRequestDisposable.dispose();
      resumeSaveStreamDisposable.dispose();
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

  private void requestN() {
    long requests = Math.min(downStreamRequestN, resumeSaveStreamRequestN);
    if (requests > 0) {
      downStreamRequestN -= requests;
      resumeSaveStreamRequestN -= requests;
      subs.forEach(sub -> sub.request(requests));
    }
  }
}
