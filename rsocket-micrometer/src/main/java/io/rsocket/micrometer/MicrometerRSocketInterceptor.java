/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.micrometer;

import io.micrometer.api.instrument.Meter;
import io.micrometer.api.instrument.MeterRegistry;
import io.micrometer.api.instrument.Tag;
import io.rsocket.RSocket;
import io.rsocket.plugins.RSocketInterceptor;
import java.util.Objects;
import reactor.core.publisher.SignalType;

/**
 * An implementation of {@link RSocketInterceptor} that intercepts interactions and gathers
 * Micrometer metrics about them.
 *
 * <p>The metrics are called {@code rsocket.[ metadata.push | request.channel | request.fnf |
 * request.response | request.stream ]} and is tagged with {@code signal.type} ({@link SignalType})
 * and any additional configured tags.
 *
 * @see <a href="https://micrometer.io">Micrometer</a>
 */
public final class MicrometerRSocketInterceptor implements RSocketInterceptor {

  private final MeterRegistry meterRegistry;

  private final Tag[] tags;

  /**
   * Creates a new {@link RSocketInterceptor}.
   *
   * @param meterRegistry the {@link MeterRegistry} to use to create {@link Meter}s.
   * @param tags the additional tags to attach to each {@link Meter}
   * @throws NullPointerException if {@code meterRegistry} is {@code null}
   */
  public MicrometerRSocketInterceptor(MeterRegistry meterRegistry, Tag... tags) {
    this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
    this.tags = tags;
  }

  @Override
  public MicrometerRSocket apply(RSocket delegate) {
    Objects.requireNonNull(delegate, "delegate must not be null");

    return new MicrometerRSocket(delegate, meterRegistry, tags);
  }
}
