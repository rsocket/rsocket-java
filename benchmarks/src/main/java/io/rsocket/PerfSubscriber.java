package io.rsocket;

import java.util.concurrent.CountDownLatch;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

public class PerfSubscriber<T> extends CountDownLatch implements CoreSubscriber<T> {

  final Blackhole blackhole;

  Subscription s;

  public PerfSubscriber(Blackhole blackhole) {
    super(1);
    this.blackhole = blackhole;
  }

  @Override
  public void onSubscribe(Subscription s) {
    this.s = s;
    s.request(1);
  }

  @Override
  public void onNext(T payload) {
    blackhole.consume(payload);
    s.request(1);
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
