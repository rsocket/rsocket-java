package io.rsocket.resume;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;

public class RequestListener {
  private final ReplayProcessor<Long> requests = ReplayProcessor.create(1);

  public <T> Flux<T> apply(Flux<T> flux) {
    return flux.doOnRequest(requests::onNext);
  }

  public Flux<Long> requests() {
    return requests;
  }
}
