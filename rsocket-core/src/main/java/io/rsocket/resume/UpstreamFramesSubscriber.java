package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class UpstreamFramesSubscriber implements Subscriber<ByteBuf>,Disposable {
  private final AtomicBoolean disposed = new AtomicBoolean();
  private final AtomicBoolean subscribed = new AtomicBoolean();

  private final int firstRequestSize;
  private final int requestSize;

  private long received;
  private final Consumer<ByteBuf> itemConsumer;
  private volatile Subscription subs;

  private boolean resumeStarted;
  private final Queue<ByteBuf> framesCache;
  private boolean pendingRequest;

  UpstreamFramesSubscriber(int limitRate,
                           Consumer<ByteBuf> itemConsumer,
                           Queue<ByteBuf> framesCache) {
    this.requestSize = limitRate;
    this.firstRequestSize = requestSize * 2;
    this.itemConsumer = itemConsumer;
    this.framesCache = framesCache;
  }

  @Override
  public void onSubscribe(Subscription s) {
    this.subs = s;
    if (subscribed.compareAndSet(false, true)) {
      if (!isDisposed()) {
        s.request(firstRequestSize);
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

  @Override
  public void onNext(ByteBuf item) {
    received++;

    if (resumeStarted) {
      framesCache.offer(item);
    } else {
      itemConsumer.accept(item);
    }

    if (received % requestSize == 0) {
      if (!resumeStarted) {
        subs.request(requestSize);
      } else {
        pendingRequest = true;
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
    if (pendingRequest) {
      pendingRequest = false;
      subs.request(requestSize);
    }
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
