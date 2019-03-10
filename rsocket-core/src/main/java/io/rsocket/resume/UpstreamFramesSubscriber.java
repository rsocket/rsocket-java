package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

class UpstreamFramesSubscriber implements Subscriber<ByteBuf>, Disposable {
  private static final Logger logger = LoggerFactory.getLogger(UpstreamFramesSubscriber.class);

  private final AtomicBoolean disposed = new AtomicBoolean();
  private final Consumer<ByteBuf> itemConsumer;
  private final Disposable downstreamRequestDisposable;
  private final Disposable resumeSaveStreamDisposable;

  private Subscription subs;
  private boolean resumeStarted;
  private final Queue<ByteBuf> framesCache;
  private long request;
  private long downStreamRequestN;
  private long resumeSaveStreamRequestN;

  UpstreamFramesSubscriber(int estimatedDownstreamRequest,
                           Flux<Long> downstreamRequests,
                           Flux<Long> resumeSaveStreamRequests,
                           Consumer<ByteBuf> itemConsumer) {
    this.itemConsumer = itemConsumer;
    this.framesCache = Queues.<ByteBuf>unbounded(estimatedDownstreamRequest).get();

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

  @Override
  public void onSubscribe(Subscription s) {
    this.subs = s;
    if (!isDisposed()) {
      doRequest();
    } else {
      s.cancel();
    }
  }

  private void requestN() {
    long requests = Math.min(downStreamRequestN, resumeSaveStreamRequestN);
    if (requests > 0) {
      downStreamRequestN -= requests;
      resumeSaveStreamRequestN -= requests;
      logger.info("Upstream subscriber requestN: {}", requests);
      request = Operators.addCap(request, requests);
      doRequest();
    }
  }

  private void doRequest() {
    if (subs != null && !resumeStarted) {
      if (request > 0) {
        subs.request(request);
        request = 0;
      }
    }
  }

  @Override
  public void onNext(ByteBuf item) {
    if (resumeStarted) {
      framesCache.offer(item);
    } else {
      itemConsumer.accept(item);
    }
  }

  @Override
  public void onError(Throwable t) {
    dispose();
  }

  @Override
  public void onComplete() {
    dispose();
  }

  public void resumeStart() {
    resumeStarted = true;
  }

  public void resumeComplete() {
    resumeStarted = false;
    ByteBuf frame = framesCache.poll();
    while (frame != null) {
      itemConsumer.accept(frame);
      frame = framesCache.poll();
    }
    doRequest();
  }

  @Override
  public void dispose() {
    if (disposed.compareAndSet(false, true)) {
      releaseCache();
      if (subs != null) {
        subs.cancel();
      }
      resumeSaveStreamDisposable.dispose();
      downstreamRequestDisposable.dispose();
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
