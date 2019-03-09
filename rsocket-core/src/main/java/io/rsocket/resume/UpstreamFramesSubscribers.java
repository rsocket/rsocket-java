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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

class UpstreamFramesSubscribers implements Disposable {
  private static final Logger logger = LoggerFactory.getLogger(UpstreamFramesSubscribers.class);
  private static final int INITIAL_SUBSCRIBER_REQUEST = 128;

  private final List<UpstreamFramesSubscriber> subs = new ArrayList<>();
  private final AtomicBoolean disposed = new AtomicBoolean();

  private final Queue<ByteBuf> framesCache;
  private final Consumer<ByteBuf> frameConsumer;
  private final Disposable downstreamRequestDisposable;
  private final Disposable resumeSaveStreamDisposable;
  private long downStreamRequestN;
  private long resumeSaveStreamRequestN;
  private long requestCredit;
  private long curRequests;

  public UpstreamFramesSubscribers(
      Flux<Long> downstreamRequests,
      Flux<Long> resumeSaveStreamRequests,
      int estimatedDownstreamRequestN,
      Consumer<ByteBuf> frameConsumer) {
    this.framesCache = Queues.<ByteBuf>unbounded(
        estimatedDownstreamRequestN).get();
    this.frameConsumer = frameConsumer;
    this.requestCredit = estimatedDownstreamRequestN;

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
        this,
        curRequests > 0
                ? Math.max(1, curRequests / Math.max(1, subs.size()))
                : INITIAL_SUBSCRIBER_REQUEST,
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

  /*used to avoid stalled streams when some of recipients of requestN (upstream RSockets) do
  * not produce items hence no new requestN are sent by downstream (DuplexConnection.send)*/
  public void overRequest(UpstreamFramesSubscriber subscriber) {
    if (requestCredit > 0) {
      logger.info("Credit: {}, request: {}", requestCredit, subscriber.requestCount());
      long actualRequest = Math.min(requestCredit, subscriber.requestCount());
      requestCredit -= actualRequest;
      subscriber.overRequest(actualRequest);
    }
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
    long originalRequests = requests;

    requestCredit = requests;

    downStreamRequestN -= requests;
    resumeSaveStreamRequestN -= requests;

    if (requests > 0) {
      int subsSize = subs.size();
      long totalRequestCount = 0;
      for (int i = 0; i < subsSize; i++) {
        UpstreamFramesSubscriber sub = subs.get(i);
        totalRequestCount += sub.requestCount();
      }

      requests -= totalRequestCount;
      if (requests >= 0) {
        long baseExcess = requests / subsSize;
        long rem = requests % subsSize;

        for (int i = 0; i < subsSize; i++) {
          UpstreamFramesSubscriber sub = subs.get(i);
          long excess = (i < rem) ? baseExcess + 1 : baseExcess;
          long totalRequestN = sub.requestCount() + excess;
          if (totalRequestN > 0) {
            sub.request(totalRequestN);
          }
        }
      } else {
        requests = originalRequests;
        for (int i = 0; i < subsSize; i++) {
          UpstreamFramesSubscriber sub = subs.get(i);
          //last subscriber
          if (i == subsSize - 1) {
            sub.request(requests);
          } else {
            long request = (long) (requests * (sub.requestCount() / (float) totalRequestCount));
            requests -= request;
            sub.request(request);
          }
        }
      }
      curRequests = requests;
    }
  }
}
