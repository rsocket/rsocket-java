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

package io.rsocket.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class WellKnownMimeTypeTest {

  @Test
  void fromIdentifierGreaterThan127() {
    assertThat(WellKnownMimeType.fromIdentifier(128))
        .isSameAs(WellKnownMimeType.UNPARSEABLE_MIME_TYPE);
  }

  @Test
  void fromIdentifierMatchFromMimeType() {
    for (WellKnownMimeType mimeType : WellKnownMimeType.values()) {
      if (mimeType == WellKnownMimeType.UNPARSEABLE_MIME_TYPE
          || mimeType == WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE) {
        continue;
      }
      assertThat(WellKnownMimeType.fromString(mimeType.toString()))
          .as("mimeType string for " + mimeType.name())
          .isSameAs(mimeType);

      assertThat(WellKnownMimeType.fromIdentifier(mimeType.getIdentifier()))
          .as("mimeType ID for " + mimeType.name())
          .isSameAs(mimeType);
    }
  }

  @Test
  void fromIdentifierNegative() {
    assertThat(WellKnownMimeType.fromIdentifier(-1))
        .isSameAs(WellKnownMimeType.fromIdentifier(-2))
        .isSameAs(WellKnownMimeType.fromIdentifier(-12))
        .isSameAs(WellKnownMimeType.UNPARSEABLE_MIME_TYPE);
  }

  @Test
  void fromIdentifierReserved() {
    assertThat(WellKnownMimeType.fromIdentifier(120))
        .isSameAs(WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE);
  }

  @Test
  void fromStringUnknown() {
    assertThat(WellKnownMimeType.fromString("foo/bar"))
        .isSameAs(WellKnownMimeType.UNPARSEABLE_MIME_TYPE);
  }

  @Test
  void fromStringUnknownReservedStillReturnsUnparseable() {
    assertThat(
            WellKnownMimeType.fromString(WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE.getString()))
        .isSameAs(WellKnownMimeType.UNPARSEABLE_MIME_TYPE);
  }
}
