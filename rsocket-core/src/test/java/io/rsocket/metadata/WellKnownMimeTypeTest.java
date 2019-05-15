package io.rsocket.metadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Simon Baslé
 */
class WellKnownMimeTypeTest {

    @Test
    void fromIdMatchFromMimeType() {
        for (WellKnownMimeType mimeType : WellKnownMimeType.values()) {
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
        assertThatIllegalArgumentException().isThrownBy(() ->
                WellKnownMimeType.fromId(-1))
                .withMessage("WellKnownMimeType IDs are between 0 and 127, inclusive");
    }

    @Test
    void fromIdGreaterThan127() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                WellKnownMimeType.fromId(128))
                .withMessage("WellKnownMimeType IDs are between 0 and 127, inclusive");
    }

    @Test
    void fromIdReserved() {
        assertThatIllegalStateException().isThrownBy(() ->
                WellKnownMimeType.fromId(120))
                .withMessage("120 is between 0 and 127 yet no WellKnownMimeType found");
    }

    @Test
    void fromMimeTypeUnknown() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                WellKnownMimeType.fromMimeType("foo/bar"))
                .withMessage("not a WellKnownMimeType: foo/bar");
    }

}