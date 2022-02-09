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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;

import io.micrometer.api.instrument.simple.SimpleMeterRegistry;
import io.rsocket.RSocket;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class MicrometerRSocketInterceptorTest {

  private final RSocket delegate = mock(RSocket.class, RETURNS_SMART_NULLS);

  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

  @DisplayName("creates MicrometerRSocket")
  @Test
  void apply() {
    assertThat(new MicrometerRSocketInterceptor(meterRegistry).apply(delegate))
        .isInstanceOf(MicrometerRSocket.class);
  }

  @DisplayName("apply throws NullPointerException with null delegate")
  @Test
  void applyNullDelegate() {
    assertThatNullPointerException()
        .isThrownBy(() -> new MicrometerRSocketInterceptor(meterRegistry).apply(null))
        .withMessage("delegate must not be null");
  }

  @DisplayName("constructor throws NullPointerException with null meterRegistry")
  @Test
  void constructorNullMeterRegistry() {
    assertThatNullPointerException()
        .isThrownBy(() -> new MicrometerRSocketInterceptor(null))
        .withMessage("meterRegistry must not be null");
  }
}
