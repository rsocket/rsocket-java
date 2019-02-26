package io.rsocket.resume;

import java.time.Duration;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ExponentialBackoffResumeStrategy implements ResumeStrategy {
  private volatile Duration next;
  private final Duration firstBackoff;
  private final Duration maxBackoff;
  private final int factor;

  public ExponentialBackoffResumeStrategy(Duration firstBackoff, Duration maxBackoff, int factor) {
    this.firstBackoff = Objects.requireNonNull(firstBackoff, "firstBackoff");
    this.maxBackoff = Objects.requireNonNull(maxBackoff, "maxBackoff");
    this.factor = requirePositive(factor);
  }

  @Override
  public Publisher<?> apply(ClientResume clientResume, Throwable throwable) {
    return Flux.defer(() -> Mono.delay(next()).thenReturn(toString()));
  }

  Duration next() {
    next =
        next == null
            ? firstBackoff
            : Duration.ofMillis(Math.min(maxBackoff.toMillis(), next.toMillis() * factor));
    return next;
  }

  private static int requirePositive(int value) {
    if (value <= 0) {
      throw new IllegalArgumentException("Value must be positive: " + value);
    } else {
      return value;
    }
  }

  @Override
  public String toString() {
    return "ExponentialBackoffResumeStrategy{"
        + "next="
        + next
        + ", firstBackoff="
        + firstBackoff
        + ", maxBackoff="
        + maxBackoff
        + ", factor="
        + factor
        + '}';
  }
}
