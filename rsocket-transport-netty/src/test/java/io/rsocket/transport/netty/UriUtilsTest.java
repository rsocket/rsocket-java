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

package io.rsocket.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.net.URI;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class UriUtilsTest {

  @DisplayName("returns the port")
  @Test
  void getPort() {
    assertThat(UriUtils.getPort(URI.create("http://localhost:42"), Integer.MAX_VALUE))
        .isEqualTo(42);
  }

  @DisplayName("getPort throws NullPointerException with null uri")
  @Test
  void getPortNullUri() {
    assertThatNullPointerException()
        .isThrownBy(() -> UriUtils.getPort(null, 80))
        .withMessage("uri must not be null");
  }

  @DisplayName("returns the default port")
  @Test
  void getPortUnset() {
    assertThat(UriUtils.getPort(URI.create("http://localhost"), Integer.MAX_VALUE))
        .isEqualTo(Integer.MAX_VALUE);
  }

  @DisplayName("returns the URI's secureness")
  @Test
  void isSecure() {
    assertThat(UriUtils.isSecure(URI.create("http://localhost"))).isFalse();
    assertThat(UriUtils.isSecure(URI.create("ws://localhost"))).isFalse();

    assertThat(UriUtils.isSecure(URI.create("https://localhost"))).isTrue();
    assertThat(UriUtils.isSecure(URI.create("wss://localhost"))).isTrue();
  }

  @DisplayName("isSecure throws NullPointerException with null uri")
  @Test
  void isSecureNullUri() {
    assertThatNullPointerException()
        .isThrownBy(() -> UriUtils.isSecure(null))
        .withMessage("uri must not be null");
  }
}
