package io.rsocket.framing;

import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class LengthUtilsTest {

  @DisplayName("getLengthAsUnsignedByte returns length if 255")
  @Test
  void getLengthAsUnsignedByte() {
    assertThat(LengthUtils.getLengthAsUnsignedByte(getRandomByteBuf((1 << 8) - 1))).isEqualTo(255);
  }

  @DisplayName("getLengthAsUnsignedByte throws NullPointerException with null byteBuf")
  @Test
  void getLengthAsUnsignedByteNullByteBuf() {
    assertThatNullPointerException()
        .isThrownBy(() -> LengthUtils.getLengthAsUnsignedByte(null))
        .withMessage("byteBuf must not be null");
  }

  @DisplayName("getLengthAsUnsignedByte throws IllegalArgumentException if larger than 255")
  @Test
  void getLengthAsUnsignedByteOverFlow() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> LengthUtils.getLengthAsUnsignedByte(getRandomByteBuf(1 << 8)))
        .withMessage("%d is larger than 8 bits", 1 << 8);
  }

  @DisplayName("getLengthAsUnsignedShort throws NullPointerException with null byteBuf")
  @Test
  void getLengthAsUnsignedIntegerNullByteBuf() {
    assertThatNullPointerException()
        .isThrownBy(() -> LengthUtils.getLengthAsUnsignedShort(null))
        .withMessage("byteBuf must not be null");
  }

  @DisplayName("getLengthAsUnsignedMedium returns length if 16_777_215")
  @Test
  void getLengthAsUnsignedMedium() {
    assertThat(LengthUtils.getLengthAsUnsignedMedium(getRandomByteBuf((1 << 24) - 1)))
        .isEqualTo(16_777_215);
  }

  @DisplayName("getLengthAsUnsignedMedium throws NullPointerException with null byteBuf")
  @Test
  void getLengthAsUnsignedMediumNullByteBuf() {
    assertThatNullPointerException()
        .isThrownBy(() -> LengthUtils.getLengthAsUnsignedMedium(null))
        .withMessage("byteBuf must not be null");
  }

  @DisplayName(
      "getLengthAsUnsignedMedium throws IllegalArgumentException if larger than 16_777_215")
  @Test
  void getLengthAsUnsignedMediumOverFlow() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> LengthUtils.getLengthAsUnsignedMedium(getRandomByteBuf(1 << 24)))
        .withMessage("%d is larger than 24 bits", 1 << 24);
  }

  @DisplayName("getLengthAsUnsignedShort returns length if 65_535")
  @Test
  void getLengthAsUnsignedShort() {
    assertThat(LengthUtils.getLengthAsUnsignedShort(getRandomByteBuf((1 << 16) - 1)))
        .isEqualTo(65_535);
  }

  @DisplayName("getLengthAsUnsignedShort throws IllegalArgumentException if larger than 65_535")
  @Test
  void getLengthAsUnsignedShortOverFlow() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> LengthUtils.getLengthAsUnsignedShort(getRandomByteBuf(1 << 16)))
        .withMessage("%d is larger than 16 bits", 1 << 16);
  }
}
