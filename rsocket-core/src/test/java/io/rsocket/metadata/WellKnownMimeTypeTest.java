package io.rsocket.metadata;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

class WellKnownMimeTypeTest {

  @Test
  void fromIdMatchFromMimeType() {
    for (WellKnownMimeType mimeType : WellKnownMimeType.values()) {
      if (mimeType == WellKnownMimeType.UNPARSEABLE_MIME_TYPE
          || mimeType == WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE) {
        continue;
      }
      assertThat(WellKnownMimeType.fromMimeType(mimeType.toString()))
          .as("mimeType string for " + mimeType.name())
          .isSameAs(mimeType);

      assertThat(WellKnownMimeType.fromId(mimeType.getIdentifier()))
          .as("mimeType ID for " + mimeType.name())
          .isSameAs(mimeType);
    }
  }

  @Test
  void fromIdNegative() {
    assertThat(WellKnownMimeType.fromId(-1))
        .isSameAs(WellKnownMimeType.fromId(-2))
        .isSameAs(WellKnownMimeType.fromId(-12))
        .isSameAs(WellKnownMimeType.UNPARSEABLE_MIME_TYPE);
  }

  @Test
  void fromIdGreaterThan127() {
    assertThat(WellKnownMimeType.fromId(128)).isSameAs(WellKnownMimeType.UNPARSEABLE_MIME_TYPE);
  }

  @Test
  void fromIdReserved() {
    assertThat(WellKnownMimeType.fromId(120))
        .isSameAs(WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE);
  }

  @Test
  void fromMimeTypeUnknown() {
    assertThat(WellKnownMimeType.fromMimeType("foo/bar"))
        .isSameAs(WellKnownMimeType.UNPARSEABLE_MIME_TYPE);
  }

  @Test
  void fromMimeTypeUnkwnowReservedStillReturnsUnparseable() {
    assertThat(
            WellKnownMimeType.fromMimeType(WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE.getMime()))
        .isSameAs(WellKnownMimeType.UNPARSEABLE_MIME_TYPE);
  }
}
