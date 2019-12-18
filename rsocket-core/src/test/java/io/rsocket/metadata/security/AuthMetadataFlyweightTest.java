package io.rsocket.metadata.security;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class AuthMetadataFlyweightTest {

  public static final int AUTH_TYPE_ID_LENGTH = 1;
  public static final int USER_NAME_BYTES_LENGTH = 1;
  public static final String TEST_BEARER_TOKEN =
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJpYXQxIjoxNTE2MjM5MDIyLCJpYXQyIjoxNTE2MjM5MDIyLCJpYXQzIjoxNTE2MjM5MDIyLCJpYXQ0IjoxNTE2MjM5MDIyfQ.ljYuH-GNyyhhLcx-rHMchRkGbNsR2_4aSxo8XjrYrSM";

  @Test
  void shouldCorrectlyEncodeData() {
    String username = "test";
    String password = "tset1234";

    int usernameLength = username.length();
    int passwordLength = password.length();

    ByteBuf byteBuf =
        AuthMetadataFlyweight.encodeSimpleMetadata(
            ByteBufAllocator.DEFAULT, username.toCharArray(), password.toCharArray());

    checkSimpleAuthMetadataEncoding(username, password, usernameLength, passwordLength, byteBuf);
  }

  @Test
  void shouldCorrectlyEncodeData1() {
    String username = "𠜎𠜱𠝹𠱓𠱸𠲖𠳏𠳕𠴕𠵼𠵿𠸎";
    String password = "tset1234";

    int usernameLength = username.getBytes(CharsetUtil.UTF_8).length;
    int passwordLength = password.length();

    ByteBuf byteBuf =
        AuthMetadataFlyweight.encodeSimpleMetadata(
            ByteBufAllocator.DEFAULT, username.toCharArray(), password.toCharArray());

    checkSimpleAuthMetadataEncoding(username, password, usernameLength, passwordLength, byteBuf);
  }

  @Test
  void shouldCorrectlyEncodeData2() {
    String username = "𠜎𠜱𠝹𠱓𠱸𠲖𠳏𠳕𠴕𠵼𠵿𠸎1234567#4? ";
    String password = "tset1234";

    int usernameLength = username.getBytes(CharsetUtil.UTF_8).length;
    int passwordLength = password.length();

    ByteBuf byteBuf =
        AuthMetadataFlyweight.encodeSimpleMetadata(
            ByteBufAllocator.DEFAULT, username.toCharArray(), password.toCharArray());

    checkSimpleAuthMetadataEncoding(username, password, usernameLength, passwordLength, byteBuf);
  }

  private static void checkSimpleAuthMetadataEncoding(
      String username, String password, int usernameLength, int passwordLength, ByteBuf byteBuf) {
    Assertions.assertThat(byteBuf.capacity())
        .isEqualTo(AUTH_TYPE_ID_LENGTH + USER_NAME_BYTES_LENGTH + usernameLength + passwordLength);

    Assertions.assertThat(byteBuf.readUnsignedByte() & ~0x80)
        .isEqualTo(WellKnownAuthType.SIMPLE.getIdentifier());
    Assertions.assertThat(byteBuf.readUnsignedByte()).isEqualTo((short) usernameLength);

    Assertions.assertThat(byteBuf.readCharSequence(usernameLength, CharsetUtil.UTF_8))
        .isEqualTo(username);
    Assertions.assertThat(byteBuf.readCharSequence(passwordLength, CharsetUtil.UTF_8))
        .isEqualTo(password);

    ReferenceCountUtil.release(byteBuf);
  }

  @Test
  void shouldThrowExceptionIfUsernameLengthExitsAllowedBounds() {
    String username =
        "𠜎𠜱𠝹𠱓𠱸𠲖𠳏𠳕𠴕𠵼𠵿𠸎𠸏𠹷𠺝𠺢𠻗𠻹𠻺𠼭𠼮𠽌𠾴𠾼𠿪𡁜𡁯𡁵𡁶𡁻𡃁𡃉𡇙𢃇𢞵𢫕𢭃𢯊𢱑𢱕𢳂𢴈𢵌𢵧𢺳𣲷𤓓𤶸𤷪𥄫𦉘𦟌𦧲𦧺𧨾𨅝𨈇𨋢𨳊𨳍𨳒𩶘";
    String password = "tset1234";

    Assertions.assertThatThrownBy(
            () ->
                AuthMetadataFlyweight.encodeSimpleMetadata(
                    ByteBufAllocator.DEFAULT, username.toCharArray(), password.toCharArray()))
        .hasMessage(
            "Username should be shorter than or equal to 128 bytes length in UTF-8 encoding");
  }

  @Test
  void shouldEncodeBearerMetadata() {
    String testToken = TEST_BEARER_TOKEN;

    ByteBuf byteBuf =
        AuthMetadataFlyweight.encodeBearerMetadata(
            ByteBufAllocator.DEFAULT, testToken.toCharArray());

    checkBearerAuthMetadataEncoding(testToken, byteBuf);
  }

  private static void checkBearerAuthMetadataEncoding(String testToken, ByteBuf byteBuf) {
    Assertions.assertThat(byteBuf.capacity())
        .isEqualTo(testToken.getBytes(CharsetUtil.UTF_8).length + AUTH_TYPE_ID_LENGTH);
    Assertions.assertThat(
            byteBuf.readUnsignedByte() & ~AuthMetadataFlyweight.STREAM_METADATA_KNOWN_MASK)
        .isEqualTo(WellKnownAuthType.BEARER.getIdentifier());
    Assertions.assertThat(byteBuf.readSlice(byteBuf.capacity() - 1).toString(CharsetUtil.UTF_8))
        .isEqualTo(testToken);
  }

  @Test
  void shouldEncodeCustomAuth() {
    String payloadAsAText = "testsecuritybuffer";
    ByteBuf testSecurityPayload =
        Unpooled.wrappedBuffer(payloadAsAText.getBytes(CharsetUtil.UTF_8));

    String customAuthType = "myownauthtype";
    ByteBuf buffer =
        AuthMetadataFlyweight.encodeMetadata(
            ByteBufAllocator.DEFAULT, customAuthType, testSecurityPayload);

    checkCustomAuthMetadataEncoding(testSecurityPayload, customAuthType, buffer);
  }

  private static void checkCustomAuthMetadataEncoding(
      ByteBuf testSecurityPayload, String customAuthType, ByteBuf buffer) {
    Assertions.assertThat(buffer.capacity())
        .isEqualTo(1 + customAuthType.length() + testSecurityPayload.capacity());
    Assertions.assertThat(buffer.readUnsignedByte())
        .isEqualTo((short) (customAuthType.length() - 1));
    Assertions.assertThat(
            buffer.readCharSequence(customAuthType.length(), CharsetUtil.US_ASCII).toString())
        .isEqualTo(customAuthType);
    Assertions.assertThat(buffer.readSlice(testSecurityPayload.capacity()))
        .isEqualTo(testSecurityPayload);

    ReferenceCountUtil.release(buffer);
  }

  @Test
  void shouldThrowOnNonASCIIChars() {
    ByteBuf testSecurityPayload = ByteBufAllocator.DEFAULT.buffer();
    String customAuthType = "1234567#4? 𠜎𠜱𠝹𠱓𠱸𠲖𠳏𠳕𠴕𠵼𠵿𠸎";

    Assertions.assertThatThrownBy(
            () ->
                AuthMetadataFlyweight.encodeMetadata(
                    ByteBufAllocator.DEFAULT, customAuthType, testSecurityPayload))
        .hasMessage("custom auth type must be US_ASCII characters only");

    Assertions.assertThat(testSecurityPayload.refCnt()).isZero();
  }

  @Test
  void shouldThrowOnOutOfAllowedSizeType() {
    ByteBuf testSecurityPayload = ByteBufAllocator.DEFAULT.buffer();
    // 130 chars
    String customAuthType =
        "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    Assertions.assertThatThrownBy(
            () ->
                AuthMetadataFlyweight.encodeMetadata(
                    ByteBufAllocator.DEFAULT, customAuthType, testSecurityPayload))
        .hasMessage(
            "custom auth type must have a strictly positive length that fits on 7 unsigned bits, ie 1-128");

    Assertions.assertThat(testSecurityPayload.refCnt()).isZero();
  }

  @Test
  void shouldThrowOnOutOfAllowedSizeType1() {
    ByteBuf testSecurityPayload = ByteBufAllocator.DEFAULT.buffer();
    String customAuthType = "";

    Assertions.assertThatThrownBy(
            () ->
                AuthMetadataFlyweight.encodeMetadata(
                    ByteBufAllocator.DEFAULT, customAuthType, testSecurityPayload))
        .hasMessage(
            "custom auth type must have a strictly positive length that fits on 7 unsigned bits, ie 1-128");

    Assertions.assertThat(testSecurityPayload.refCnt()).isZero();
  }

  @Test
  void shouldEncodeUsingWellKnownAuthType() {
    ByteBuf byteBuf =
        AuthMetadataFlyweight.encodeMetadata(
            ByteBufAllocator.DEFAULT,
            WellKnownAuthType.SIMPLE,
            ByteBufAllocator.DEFAULT.buffer(3, 3).writeByte(1).writeByte('u').writeByte('p'));

    checkSimpleAuthMetadataEncoding("u", "p", 1, 1, byteBuf);
  }

  @Test
  void shouldEncodeUsingWellKnownAuthType1() {
    ByteBuf byteBuf =
        AuthMetadataFlyweight.encodeMetadata(
            ByteBufAllocator.DEFAULT,
            WellKnownAuthType.SIMPLE,
            ByteBufAllocator.DEFAULT.buffer().writeByte(1).writeByte('u').writeByte('p'));

    checkSimpleAuthMetadataEncoding("u", "p", 1, 1, byteBuf);
  }

  @Test
  void shouldEncodeUsingWellKnownAuthType2() {
    ByteBuf byteBuf =
        AuthMetadataFlyweight.encodeMetadata(
            ByteBufAllocator.DEFAULT,
            WellKnownAuthType.BEARER,
            Unpooled.copiedBuffer(TEST_BEARER_TOKEN, CharsetUtil.UTF_8));

    checkBearerAuthMetadataEncoding(TEST_BEARER_TOKEN, byteBuf);
  }

  @Test
  void shouldThrowIfWellKnownAuthTypeIsUnsupportedOrUnknown() {
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer().retain();
    Assertions.assertThat(buffer.refCnt()).isEqualTo(2);

    Assertions.assertThatThrownBy(
        () ->
            AuthMetadataFlyweight.encodeMetadata(
                ByteBufAllocator.DEFAULT, WellKnownAuthType.UNPARSEABLE_AUTH_TYPE, buffer));
    Assertions.assertThat(buffer.refCnt()).isOne();

    Assertions.assertThatThrownBy(
        () ->
            AuthMetadataFlyweight.encodeMetadata(
                ByteBufAllocator.DEFAULT, WellKnownAuthType.UNPARSEABLE_AUTH_TYPE, buffer));
    Assertions.assertThat(buffer.refCnt()).isZero();
  }

  @Test
  void shouldCompressMetadata() {
    ByteBuf byteBuf =
        AuthMetadataFlyweight.encodeMetadataWithCompression(
            ByteBufAllocator.DEFAULT,
            "simple",
            ByteBufAllocator.DEFAULT.buffer().writeByte(1).writeByte('u').writeByte('p'));

    checkSimpleAuthMetadataEncoding("u", "p", 1, 1, byteBuf);
  }

  @Test
  void shouldCompressMetadata1() {
    ByteBuf byteBuf =
        AuthMetadataFlyweight.encodeMetadataWithCompression(
            ByteBufAllocator.DEFAULT,
            "bearer",
            Unpooled.copiedBuffer(TEST_BEARER_TOKEN, CharsetUtil.UTF_8));

    checkBearerAuthMetadataEncoding(TEST_BEARER_TOKEN, byteBuf);
  }

  @Test
  void shouldNotCompressMetadata() {
    ByteBuf testMetadataPayload =
        Unpooled.wrappedBuffer(TEST_BEARER_TOKEN.getBytes(CharsetUtil.UTF_8));
    String customAuthType = "testauthtype";
    ByteBuf byteBuf =
        AuthMetadataFlyweight.encodeMetadataWithCompression(
            ByteBufAllocator.DEFAULT, customAuthType, testMetadataPayload);

    checkCustomAuthMetadataEncoding(testMetadataPayload, customAuthType, byteBuf);
  }
}
