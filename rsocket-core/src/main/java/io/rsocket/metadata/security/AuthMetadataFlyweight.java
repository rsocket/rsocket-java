package io.rsocket.metadata.security;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.rsocket.buffer.TupleByteBuf;
import io.rsocket.util.CharByteBufUtil;

public class AuthMetadataFlyweight {

  static final int STREAM_METADATA_KNOWN_MASK = 0x80; // 1000 0000
  static final byte STREAM_METADATA_LENGTH_MASK = 0x7F; // 0111 1111

  static final int USERNAME_BYTES_LENGTH = 1;
  static final int AUTH_TYPE_ID_LENGTH = 1;

  private AuthMetadataFlyweight() {}

  /**
   * Encode a Authentication CompositeMetadata payload using custom authentication type
   *
   * @throws IllegalArgumentException if customAuthType is non US_ASCII string if customAuthType is
   *     empty string if customAuthType is has length greater than 128 bytes
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param customAuthType the custom mime type to encode.
   * @param metadata the metadata value to encode.
   */
  public static ByteBuf encodeMetadata(
      ByteBufAllocator allocator, String customAuthType, ByteBuf metadata) {

    int actualASCIILength = ByteBufUtil.utf8Bytes(customAuthType);
    if (actualASCIILength != customAuthType.length()) {
      metadata.release();
      throw new IllegalArgumentException("custom auth type must be US_ASCII characters only");
    }
    if (actualASCIILength < 1 || actualASCIILength > 128) {
      metadata.release();
      throw new IllegalArgumentException(
          "custom auth type must have a strictly positive length that fits on 7 unsigned bits, ie 1-128");
    }

    int capacity = 1 + actualASCIILength;
    ByteBuf headerBuffer = allocator.buffer(capacity, capacity);
    // encoded length is one less than actual length, since 0 is never a valid length, which gives
    // wider representation range
    headerBuffer.writeByte(actualASCIILength - 1);

    ByteBufUtil.reserveAndWriteUtf8(headerBuffer, customAuthType, actualASCIILength);

    return TupleByteBuf.of(allocator, headerBuffer, metadata);
  }

  /**
   * Encode a Authentication CompositeMetadata payload using custom authentication type
   *
   * @throws IllegalArgumentException if customAuthType is non US_ASCII string if customAuthType is
   *     empty string if customAuthType is has length greater than 128 bytes
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param authType the custom mime type to encode.
   * @param metadata the metadata value to encode.
   */
  public static ByteBuf encodeMetadata(
      ByteBufAllocator allocator, WellKnownAuthType authType, ByteBuf metadata) {

    if (authType == WellKnownAuthType.UNPARSEABLE_AUTH_TYPE
        || authType == WellKnownAuthType.UNKNOWN_RESERVED_AUTH_TYPE) {
      metadata.release();
      throw new IllegalArgumentException("only allowed AuthType should be used");
    }

    int capacity = AUTH_TYPE_ID_LENGTH;
    ByteBuf headerBuffer =
        allocator
            .buffer(capacity, capacity)
            .writeByte(authType.getIdentifier() | STREAM_METADATA_KNOWN_MASK);

    return TupleByteBuf.of(allocator, headerBuffer, metadata);
  }

  /**
   * Encode a Authentication CompositeMetadata payload using Simple Authentication format
   *
   * @throws IllegalArgumentException if username length is greater than 128
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param username the char sequence which represents user name.
   * @param password the char sequence which represents user password.
   */
  public static ByteBuf encodeSimpleMetadata(
      ByteBufAllocator allocator, char[] username, char[] password) {

    int usernameLength = CharByteBufUtil.utf8Bytes(username);
    if (usernameLength > 128) {
      throw new IllegalArgumentException(
          "Username should be shorter than or equal to 128 bytes length in UTF-8 encoding");
    }

    int passwordLength = CharByteBufUtil.utf8Bytes(password);
    int capacity = AUTH_TYPE_ID_LENGTH + USERNAME_BYTES_LENGTH + usernameLength + passwordLength;
    final ByteBuf buffer =
        allocator
            .buffer(capacity, capacity)
            .writeByte(WellKnownAuthType.SIMPLE.getIdentifier() | STREAM_METADATA_KNOWN_MASK)
            .writeByte(usernameLength);

    CharByteBufUtil.writeUtf8(buffer, username);
    CharByteBufUtil.writeUtf8(buffer, password);

    return buffer;
  }

  /**
   * Encode a Authentication CompositeMetadata payload using Bearer Authentication format
   *
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param token the char sequence which represents BEARER token.
   */
  public static ByteBuf encodeBearerMetadata(ByteBufAllocator allocator, char[] token) {

    int tokenLength = CharByteBufUtil.utf8Bytes(token);
    int capacity = AUTH_TYPE_ID_LENGTH + tokenLength;
    final ByteBuf buffer =
        allocator
            .buffer(capacity, capacity)
            .writeByte(WellKnownAuthType.BEARER.getIdentifier() | STREAM_METADATA_KNOWN_MASK);

    CharByteBufUtil.writeUtf8(buffer, token);

    return buffer;
  }

  /**
   * Encode a new Authentication Metadata payload information, first verifying if the passed {@link
   * String} matches a {@link WellKnownAuthType} (in which case it will be encoded in a compressed
   * fashion using the mime id of that type).
   *
   * <p>Prefer using {@link #encodeMetadata(ByteBufAllocator, String, ByteBuf)} if you already know
   * that the mime type is not a {@link WellKnownAuthType}.
   *
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param authType the mime type to encode, as a {@link String}. well known mime types are
   *     compressed.
   * @param metadata the metadata value to encode.
   * @see #encodeMetadata(ByteBufAllocator, WellKnownAuthType, ByteBuf)
   * @see #encodeMetadata(ByteBufAllocator, String, ByteBuf)
   */
  public static ByteBuf encodeMetadataWithCompression(
      ByteBufAllocator allocator, String authType, ByteBuf metadata) {
    WellKnownAuthType wkn = WellKnownAuthType.fromString(authType);
    if (wkn == WellKnownAuthType.UNPARSEABLE_AUTH_TYPE) {
      return AuthMetadataFlyweight.encodeMetadata(allocator, authType, metadata);
    } else {
      return AuthMetadataFlyweight.encodeMetadata(allocator, wkn, metadata);
    }
  }
}
