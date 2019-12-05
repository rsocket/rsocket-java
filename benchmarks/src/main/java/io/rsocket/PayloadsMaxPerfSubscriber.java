package io.rsocket;

import org.openjdk.jmh.infra.Blackhole;

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
