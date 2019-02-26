package io.rsocket.resume;

import java.time.Duration;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public class PeriodicResumeStrategy implements ResumeStrategy {
  private final Duration interval;

  public PeriodicResumeStrategy(Duration interval) {
    this.interval = interval;
  }

  @Override
  public Publisher<?> apply(ClientResume clientResumeConfiguration, Throwable throwable) {
    return Mono.delay(interval).thenReturn(toString());
  }

  @Override
  public String toString() {
    return "PeriodicResumeStrategy{" + "interval=" + interval + '}';
  }
}
