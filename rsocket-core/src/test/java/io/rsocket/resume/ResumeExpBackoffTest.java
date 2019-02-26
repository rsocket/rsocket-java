package io.rsocket.resume;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

public class ResumeExpBackoffTest {

  @Test
  void backOffSeries() {
    Duration firstBackoff = Duration.ofSeconds(1);
    Duration maxBackoff = Duration.ofSeconds(32);
    int factor = 2;
    ExponentialBackoffResumeStrategy strategy =
        new ExponentialBackoffResumeStrategy(firstBackoff, maxBackoff, factor);

    List<Duration> expected =
        Flux.just(1, 2, 4, 8, 16, 32, 32).map(Duration::ofSeconds).collectList().block();

    List<Duration> actual = Flux.range(1, 7).map(v -> strategy.next()).collectList().block();

    Assertions.assertThat(actual).isEqualTo(expected);
  }

  @Test
  void nullFirstBackoff() {
    assertThrows(
        NullPointerException.class,
        () -> {
          ExponentialBackoffResumeStrategy strategy =
              new ExponentialBackoffResumeStrategy(Duration.ofSeconds(1), null, 42);
        });
  }

  @Test
  void nullMaxBackoff() {
    assertThrows(
        NullPointerException.class,
        () -> {
          ExponentialBackoffResumeStrategy strategy =
              new ExponentialBackoffResumeStrategy(null, Duration.ofSeconds(1), 42);
        });
  }

  @Test
  void negativeFactor() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ExponentialBackoffResumeStrategy strategy =
              new ExponentialBackoffResumeStrategy(
                  Duration.ofSeconds(1), Duration.ofSeconds(32), -1);
        });
  }
}
