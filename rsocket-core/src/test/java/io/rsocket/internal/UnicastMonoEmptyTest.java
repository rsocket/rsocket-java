package io.rsocket.internal;

import static io.rsocket.internal.SchedulerUtils.warmup;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.util.RaceTestUtils;

public class UnicastMonoEmptyTest {

  @Test
  public void shouldSupportASingleSubscriber() throws InterruptedException {
    warmup(Schedulers.single());

    for (int i = 0; i < 10000; i++) {
      AtomicInteger times = new AtomicInteger();
      Mono unicastMono = UnicastMonoEmpty.newInstance(times::incrementAndGet);

      Assertions.assertThatThrownBy(
              () ->
                  RaceTestUtils.race(
                      unicastMono::subscribe, unicastMono::subscribe, Schedulers.single()))
          .hasCause(new IllegalStateException("UnicastMonoEmpty allows only a single Subscriber"));
      Assertions.assertThat(times.get()).isEqualTo(1);
    }
  }

  @Test
  public void shouldSupportASingleBlock() throws InterruptedException {
    warmup(Schedulers.single());

    for (int i = 0; i < 10000; i++) {
      AtomicInteger times = new AtomicInteger();
      Mono unicastMono = UnicastMonoEmpty.newInstance(times::incrementAndGet);

      Assertions.assertThatThrownBy(
              () -> RaceTestUtils.race(unicastMono::block, unicastMono::block, Schedulers.single()))
          .hasMessage("UnicastMonoEmpty allows only a single Subscriber");
      Assertions.assertThat(times.get()).isEqualTo(1);
    }
  }

  @Test
  public void shouldSupportASingleBlockWithTimeout() throws InterruptedException {
    warmup(Schedulers.single());

    for (int i = 0; i < 10000; i++) {
      AtomicInteger times = new AtomicInteger();
      Mono unicastMono = UnicastMonoEmpty.newInstance(times::incrementAndGet);

      Assertions.assertThatThrownBy(
              () ->
                  RaceTestUtils.race(
                      () -> unicastMono.block(Duration.ofMinutes(1)),
                      () -> unicastMono.block(Duration.ofMinutes(1)),
                      Schedulers.single()))
          .hasMessage("UnicastMonoEmpty allows only a single Subscriber");
      Assertions.assertThat(times.get()).isEqualTo(1);
    }
  }

  @Test
  public void shouldSupportASingleSubscribeOrBlock() throws InterruptedException {
    warmup(Schedulers.parallel());

    for (int i = 0; i < 10000; i++) {
      AtomicInteger times = new AtomicInteger();
      Mono unicastMono = UnicastMonoEmpty.newInstance(times::incrementAndGet);

      Assertions.assertThatThrownBy(
              () ->
                  RaceTestUtils.race(
                      unicastMono::subscribe,
                      () ->
                          RaceTestUtils.race(
                              unicastMono::block,
                              () -> unicastMono.block(Duration.ofMinutes(1)),
                              Schedulers.parallel()),
                      Schedulers.parallel()))
          .matches(
              t -> {
                Assertions.assertThat(t.getSuppressed()).hasSize(2);
                Assertions.assertThat(t.getSuppressed()[0])
                    .hasMessageContaining("UnicastMonoEmpty allows only a single Subscriber");
                Assertions.assertThat(t.getSuppressed()[1])
                    .hasMessageContaining("UnicastMonoEmpty allows only a single Subscriber");

                return true;
              });
      Assertions.assertThat(times.get()).isEqualTo(1);
    }
  }
}
