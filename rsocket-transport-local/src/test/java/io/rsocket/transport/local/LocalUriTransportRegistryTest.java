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

import io.rsocket.uri.UriTransportRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class LocalUriTransportRegistryTest {

  @DisplayName("local URI returns LocalClientTransport")
  @Test
  void clientForUri() {
    assertThat(UriTransportRegistry.clientForUri("local:test1"))
        .isInstanceOf(LocalClientTransport.class);
  }

  @DisplayName("non-local URI does not return LocalClientTransport")
  @Test
  void clientForUriInvalid() {
    assertThat(UriTransportRegistry.clientForUri("http://localhost"))
        .isNotInstanceOf(LocalClientTransport.class);
  }

  @DisplayName("local URI returns LocalServerTransport")
  @Test
  void serverForUri() {
    assertThat(UriTransportRegistry.serverForUri("local:test1"))
        .isInstanceOf(LocalServerTransport.class);
  }

  @DisplayName("non-local URI does not return LocalServerTransport")
  @Test
  void serverForUriInvalid() {
    assertThat(UriTransportRegistry.serverForUri("http://localhost"))
        .isNotInstanceOf(LocalServerTransport.class);
  }
}
