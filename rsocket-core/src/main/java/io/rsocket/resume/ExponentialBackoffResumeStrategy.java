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

package io.rsocket.resume;

import java.time.Duration;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * @deprecated as of 1.0 RC7 in favor of passing {@link Retry#backoff(long, Duration)} to {@link
 *     io.rsocket.core.Resume#retry(Retry)}.
 */
@Deprecated
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
