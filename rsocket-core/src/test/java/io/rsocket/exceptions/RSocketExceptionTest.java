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

package io.rsocket.exceptions;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

interface RSocketExceptionTest<T extends RSocketException> {

  @DisplayName("constructor does not throw NullPointerException with null message")
  @Test
  default void constructorWithNullMessage() {
    assertThat(getException(null)).hasMessage(null);
  }

  @DisplayName("constructor does not throw NullPointerException with null message and cause")
  @Test
  default void constructorWithNullMessageAndCause() {
    assertThat(getException(null)).hasMessage(null);
  }

  @DisplayName("errorCode returns specified value")
  @Test
  default void errorCodeReturnsSpecifiedValue() {
    assertThat(getException("test-message").errorCode()).isEqualTo(getSpecifiedErrorCode());
  }

  T getException(String message, Throwable cause);

  T getException(String message);

  int getSpecifiedErrorCode();
}
