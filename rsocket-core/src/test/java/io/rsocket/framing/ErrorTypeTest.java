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

package io.rsocket.framing;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class ErrorTypeTest {

  @DisplayName("APPLICATION_ERROR characteristics")
  @Test
  void applicationError() {
    assertThat(ErrorType.APPLICATION_ERROR).isEqualTo(0x00000201);
  }

  @DisplayName("CANCELED characteristics")
  @Test
  void canceled() {
    assertThat(ErrorType.CANCELED).isEqualTo(0x00000203);
  }

  @DisplayName("CONNECTION_CLOSE characteristics")
  @Test
  void connectionClose() {
    assertThat(ErrorType.CONNECTION_CLOSE).isEqualTo(0x00000102);
  }

  @DisplayName("INVALID_SETUP characteristics")
  @Test
  void connectionError() {
    assertThat(ErrorType.CONNECTION_ERROR).isEqualTo(0x00000101);
  }

  @DisplayName("INVALID characteristics")
  @Test
  void invalid() {
    assertThat(ErrorType.INVALID).isEqualTo(0x00000204);
  }

  @DisplayName("INVALID_SETUP characteristics")
  @Test
  void invalidSetup() {
    assertThat(ErrorType.INVALID_SETUP).isEqualTo(0x00000001);
  }

  @DisplayName("REJECTED characteristics")
  @Test
  void rejected() {
    assertThat(ErrorType.REJECTED).isEqualTo(0x00000202);
  }

  @DisplayName("REJECTED_RESUME characteristics")
  @Test
  void rejectedResume() {
    assertThat(ErrorType.REJECTED_RESUME).isEqualTo(0x00000004);
  }

  @DisplayName("REJECTED_SETUP characteristics")
  @Test
  void rejectedSetup() {
    assertThat(ErrorType.REJECTED_SETUP).isEqualTo(0x00000003);
  }

  @DisplayName("RESERVED characteristics")
  @Test
  void reserved() {
    assertThat(ErrorType.RESERVED).isEqualTo(0x00000000);
  }

  @DisplayName("UNSUPPORTED_SETUP characteristics")
  @Test
  void unsupportedSetup() {
    assertThat(ErrorType.UNSUPPORTED_SETUP).isEqualTo(0x00000002);
  }
}
