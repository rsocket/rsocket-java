package io.rsocket.internal;

import java.util.ArrayDeque;
import java.util.Queue;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.test.util.RaceTestUtils;

class LimitableRequestPublisherTest {

  @Test
  @RepeatedTest(2)
  public void requestLimitRacingTest() throws InterruptedException {
    Queue<Long> requests = new ArrayDeque<>(10000);
    LimitableRequestPublisher<Object> limitableRequestPublisher =
        LimitableRequestPublisher.wrap(DirectProcessor.create().doOnRequest(requests::add), 0);

    Runnable request1 = () -> limitableRequestPublisher.request(1);
    Runnable request2 = () -> limitableRequestPublisher.increaseInternalLimit(2);

    limitableRequestPublisher.subscribe();

    for (int i = 0; i < 10000; i++) {
      RaceTestUtils.race(request1, request2);
    }

    Thread.sleep(1000);

    Assertions.assertThat(requests.stream().mapToLong(l -> l).sum()).isEqualTo(10000);
  }
}
