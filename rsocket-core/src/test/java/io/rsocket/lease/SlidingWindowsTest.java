/*
 * Copyright 2015-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.lease;

import static org.assertj.core.data.Percentage.withPercentage;

import java.time.Duration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class SlidingWindowsTest {

  @Test
  public void firstWindow() {
    long startMillis = System.currentTimeMillis() - 5_000;
    int firstSuccess = 13;
    int firstReject = 17;
    SlidingWindow first = SlidingWindow.first(firstSuccess, firstReject, startMillis);

    Assertions.assertThat(first.duration()).isCloseTo(5_000, withPercentage(1));
    Assertions.assertThat(first.reject()).isEqualTo(17);
    Assertions.assertThat(first.success()).isEqualTo(13);
    Assertions.assertThat(first.total()).isEqualTo(30);
    Assertions.assertThat(first.rate()).isCloseTo(30 / (float) 5_000, withPercentage(1));
  }

  @Test
  public void nextWindowIncrease() {
    long startMillis = System.currentTimeMillis();
    int firstSuccess = 13;
    int firstReject = 17;
    SlidingWindow first = SlidingWindow.first(firstSuccess, firstReject, startMillis);

    long from = System.currentTimeMillis();
    Mono.delay(Duration.ofMillis(100)).block();
    long to = System.currentTimeMillis();
    long actualDuration = to - from;

    int nextSuccess = 33;
    int nextReject = 37;
    SlidingWindow next = first.next(nextSuccess, nextReject);

    Assertions.assertThat(next.duration()).isCloseTo(actualDuration, withPercentage(10));
    Assertions.assertThat(next.reject()).isEqualTo(20);
    Assertions.assertThat(next.success()).isEqualTo(20);
    Assertions.assertThat(next.total()).isEqualTo(40);
    Assertions.assertThat(next.rate()).isCloseTo(40 / (float) actualDuration, withPercentage(10));
  }

  @Test
  public void nextWindowSame() {
    long startMillis = System.currentTimeMillis();
    int firstSuccess = 13;
    int firstReject = 17;
    SlidingWindow first = SlidingWindow.first(firstSuccess, firstReject, startMillis);

    long from = System.currentTimeMillis();
    Mono.delay(Duration.ofMillis(100)).block();
    long to = System.currentTimeMillis();
    long actualDuration = to - from;

    int nextSuccess = 13;
    int nextReject = 17;
    SlidingWindow next = first.next(nextSuccess, nextReject);

    Assertions.assertThat(next.duration()).isCloseTo(actualDuration, withPercentage(10));
    Assertions.assertThat(next.reject()).isEqualTo(0);
    Assertions.assertThat(next.success()).isEqualTo(0);
    Assertions.assertThat(next.total()).isEqualTo(0);
    Assertions.assertThat(next.rate()).isCloseTo(0, withPercentage(1));
  }

  @Test
  public void nextWindowDecrease() {
    long startMillis = System.currentTimeMillis();
    int firstSuccess = 13;
    int firstReject = 17;
    SlidingWindow first = SlidingWindow.first(firstSuccess, firstReject, startMillis);

    long from = System.currentTimeMillis();
    Mono.delay(Duration.ofMillis(100)).block();
    long to = System.currentTimeMillis();
    long actualDuration = to - from;

    int nextSuccess = 20;
    int nextReject = 19;
    SlidingWindow next = first.next(nextSuccess, nextReject);
    Assertions.assertThat(next.duration()).isCloseTo(actualDuration, withPercentage(10));
    Assertions.assertThat(next.reject()).isEqualTo(2);
    Assertions.assertThat(next.success()).isEqualTo(7);
    Assertions.assertThat(next.total()).isEqualTo(9);
    Assertions.assertThat(next.rate()).isCloseTo(9 / (float) actualDuration, withPercentage(10));
  }

  @Test
  public void multipleWindows() {
    long startMillis = System.currentTimeMillis();
    int firstSuccess = 13;
    int firstReject = 17;
    SlidingWindow first = SlidingWindow.first(firstSuccess, firstReject, startMillis);

    int nextSuccess = 20;
    int nextReject = 19;
    SlidingWindow next = first.next(nextSuccess, nextReject);

    long from = System.currentTimeMillis();
    Mono.delay(Duration.ofMillis(100)).block();
    int lastSuccess = 40;
    int lastReject = 29;
    SlidingWindow last = next.next(lastSuccess, lastReject);
    long to = System.currentTimeMillis();
    long actualDuration = to - from;

    Assertions.assertThat(last.success(first)).isEqualTo(40);
    Assertions.assertThat(last.reject(first)).isEqualTo(29);
    Assertions.assertThat(last.total(first)).isEqualTo(69);
    Assertions.assertThat(last.rate(first))
        .isCloseTo(69 / (float) actualDuration, withPercentage(10));
  }
}
