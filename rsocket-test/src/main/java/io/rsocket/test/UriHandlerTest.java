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

package io.rsocket.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.rsocket.uri.UriHandler;
import java.net.URI;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public interface UriHandlerTest {

  @DisplayName("returns empty Optional client with invalid URI")
  @Test
  default void buildClientInvalidUri() {
    assertThat(getUriHandler().buildClient(URI.create(getInvalidUri()))).isEmpty();
  }

  @DisplayName("buildClient throws NullPointerException with null uri")
  @Test
  default void buildClientNullUri() {
    assertThatNullPointerException()
        .isThrownBy(() -> getUriHandler().buildClient(null))
        .withMessage("uri must not be null");
  }

  @DisplayName("returns client with value URI")
  @Test
  default void buildClientValidUri() {
    assertThat(getUriHandler().buildClient(URI.create(getValidUri()))).isNotEmpty();
  }

  @DisplayName("returns empty Optional server with invalid URI")
  @Test
  default void buildServerInvalidUri() {
    assertThat(getUriHandler().buildServer(URI.create(getInvalidUri()))).isEmpty();
  }

  @DisplayName("buildServer throws NullPointerException with null uri")
  @Test
  default void buildServerNullUri() {
    assertThatNullPointerException()
        .isThrownBy(() -> getUriHandler().buildServer(null))
        .withMessage("uri must not be null");
  }

  @DisplayName("returns server with value URI")
  @Test
  default void buildServerValidUri() {
    assertThat(getUriHandler().buildServer(URI.create(getValidUri()))).isNotEmpty();
  }

  String getInvalidUri();

  UriHandler getUriHandler();

  String getValidUri();
}
