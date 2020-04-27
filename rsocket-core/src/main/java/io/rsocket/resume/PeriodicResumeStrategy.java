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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * @deprecated as of 1.0 RC7 in favor of passing {@link Retry#fixedDelay(long, Duration)} to {@link
 *     io.rsocket.core.Resume#retry(Retry)}.
 */
@Deprecated
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
