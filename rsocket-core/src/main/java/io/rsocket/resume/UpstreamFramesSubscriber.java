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

public class UpstreamFramesSubscriber implements Subscriber<ByteBuf>,Disposable {
  private static final Logger logger = LoggerFactory.getLogger(UpstreamFramesSubscriber.class);

  private final AtomicBoolean disposed = new AtomicBoolean();
  private final AtomicBoolean subscribed = new AtomicBoolean();

  private final Consumer<ByteBuf> itemConsumer;
  private Subscription subs;

  private boolean resumeStarted;
  private final Queue<ByteBuf> framesCache;

  private long request;

  UpstreamFramesSubscriber(Consumer<ByteBuf> itemConsumer,
                           Queue<ByteBuf> framesCache) {
    this.itemConsumer = itemConsumer;
    this.framesCache = framesCache;
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
    request = Operators.addCap(request, requestN);
    doRequest();
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
