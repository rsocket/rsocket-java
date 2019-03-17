package io.rsocket;

import java.util.concurrent.CountDownLatch;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

public class MaxPerfSubscriber implements CoreSubscriber<Payload> {

  final CountDownLatch latch = new CountDownLatch(1);
  final Blackhole blackhole;

  public MaxPerfSubscriber(Blackhole blackhole) {
    this.blackhole = blackhole;
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(Payload payload) {
    payload.release();
    blackhole.consume(payload);
  }

  @Override
  public void onError(Throwable t) {
    blackhole.consume(t);
    latch.countDown();
  }

  @Override
  public void onComplete() {
    latch.countDown();
  }
}
