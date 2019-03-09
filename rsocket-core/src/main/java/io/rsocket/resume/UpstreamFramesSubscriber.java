package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Operators;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

class UpstreamFramesSubscriber implements Subscriber<ByteBuf>, Disposable {
  private static final Logger logger = LoggerFactory.getLogger(UpstreamFramesSubscriber.class);

  private final AtomicBoolean disposed = new AtomicBoolean();
  private final AtomicBoolean subscribed = new AtomicBoolean();

  private final UpstreamFramesSubscribers upstreamFrameSubscribers;
  private final Consumer<ByteBuf> itemConsumer;
  private Subscription subs;

  private boolean resumeStarted;
  private final Queue<ByteBuf> framesCache;

  private long request;
  private long receivedItemsCount;
  private long curRequestN;
  private boolean overRequested;

  UpstreamFramesSubscriber(UpstreamFramesSubscribers upstreamFrameSubscribers,
                           long initialRequest,
                           Consumer<ByteBuf> itemConsumer,
                           Queue<ByteBuf> framesCache) {
    this.upstreamFrameSubscribers = upstreamFrameSubscribers;
    this.itemConsumer = itemConsumer;
    this.framesCache = framesCache;
    this.request = initialRequest;
  }

  @Override
  public void onSubscribe(Subscription s) {
    this.subs = s;
    if (subscribed.compareAndSet(false, true)) {
      if (!isDisposed()) {
        doRequest();
      } else {
        s.cancel();
      }
    } else {
      if (!isDisposed()) {
        onError(new IllegalStateException("FrameSubscriber must be subscribed at most once"));
        dispose();
      }
    }
  }

  public void request(long requestN) {
    doRequest(requestN, false);
  }

  public void overRequest(long requestN) {
    doRequest(requestN, true);
  }

  private void doRequest(long requestN, boolean overRequest) {
    if (requestN > 0) {
      overRequested = overRequest;
      logger.info("Upstream subscriber requestN: {}", requestN);
      request = Operators.addCap(request, requestN);
      receivedItemsCount = 0;
      curRequestN = requestN;
      doRequest();
    }
  }

  public long requestCount() {
    return receivedItemsCount;
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
    receivedItemsCount++;
    if (resumeStarted) {
      framesCache.offer(item);
    } else {
      itemConsumer.accept(item);
      if (receivedItemsCount == curRequestN && !overRequested) {
        overRequested = true;
        upstreamFrameSubscribers.overRequest(this);
      }
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
    doRequest();
  }

  @Override
  public void dispose() {
    if (disposed.compareAndSet(false, true)) {
      if (subs != null) {
        subs.cancel();
      }
    }
  }

  @Override
  public boolean isDisposed() {
    return disposed.get();
  }
}
