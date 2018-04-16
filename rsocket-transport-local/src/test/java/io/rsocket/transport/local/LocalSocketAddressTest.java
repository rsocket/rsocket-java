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

package io.rsocket.transport.local;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class LocalSocketAddressTest {

  @DisplayName("constructor throws NullPointerException with null name")
  @Test
  void constructorNullName() {
    assertThatNullPointerException()
        .isThrownBy(() -> new LocalSocketAddress(null))
        .withMessage("name must not be null");
  }

  @DisplayName("returns the configured name")
  @Test
  void name() {
    assertThat(new LocalSocketAddress("test-name").getName()).isEqualTo("test-name");
  }
}
