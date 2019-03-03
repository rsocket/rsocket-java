package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class ResumeFramesSubscriber implements Subscriber<ByteBuf>, Disposable {
  private final Flux<Long> requests;
  private final Consumer<ByteBuf> onNext;
  private final Consumer<Throwable> onError;
  private final Runnable onComplete;
  private final AtomicBoolean disposed = new AtomicBoolean();
  private volatile Disposable requestsDisposable;
  private volatile Subscription subscription;

  public ResumeFramesSubscriber(Flux<Long> requests,
                                Consumer<ByteBuf> onNext,
                                Consumer<Throwable> onError,
                                Runnable onComplete) {
    this.requests = requests;
    this.onNext = onNext;
    this.onError = onError;
    this.onComplete = onComplete;
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (isDisposed()) {
      s.cancel();
    } else {
      this.subscription = s;
      this.requestsDisposable = requests.subscribe(s::request);
    }
  }

  @Override
  public void onNext(ByteBuf frame) {
    this.onNext.accept(frame);
  }

  @Override
  public void onError(Throwable t) {
    this.onError.accept(t);
    requestsDisposable.dispose();
  }

  @Override
  public void onComplete() {
    this.onComplete.run();
    requestsDisposable.dispose();
  }

  @Override
  public void dispose() {
    if (disposed.compareAndSet(false, true)) {
      if (subscription != null) {
        subscription.cancel();
        requestsDisposable.dispose();
      }
    }
  }

  @Override
  public boolean isDisposed() {
    return disposed.get();
  }
}
