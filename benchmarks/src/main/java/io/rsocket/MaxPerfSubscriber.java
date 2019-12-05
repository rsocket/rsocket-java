package io.rsocket;

import java.util.concurrent.CountDownLatch;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

public class MaxPerfSubscriber<T> extends CountDownLatch implements CoreSubscriber<T> {

  final Blackhole blackhole;

  public MaxPerfSubscriber(Blackhole blackhole) {
    super(1);
    this.blackhole = blackhole;
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(T payload) {
    blackhole.consume(payload);
  }

  @Override
  public void onError(Throwable t) {
    blackhole.consume(t);
    countDown();
  }

  @Override
  public void onComplete() {
    countDown();
  }
}
