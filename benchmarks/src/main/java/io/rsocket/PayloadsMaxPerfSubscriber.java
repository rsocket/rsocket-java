package io.rsocket;

import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

import java.util.concurrent.CountDownLatch;

public class PayloadsMaxPerfSubscriber extends MaxPerfSubscriber<Payload> {

  public PayloadsMaxPerfSubscriber(Blackhole blackhole) {
    super(blackhole);
  }

  @Override
  public void onNext(Payload payload) {
    payload.release();
    super.onNext(payload);
  }
}
