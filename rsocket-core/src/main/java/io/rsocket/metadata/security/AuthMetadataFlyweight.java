package io.rsocket.metadata.security;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.rsocket.buffer.TupleByteBuf;
import io.rsocket.util.CharByteBufUtil;

public class AuthMetadataFlyweight {

  static final int STREAM_METADATA_KNOWN_MASK = 0x80; // 1000 0000
  static final byte STREAM_METADATA_LENGTH_MASK = 0x7F; // 0111 1111

  static final int USERNAME_BYTES_LENGTH = 1;
  static final int AUTH_TYPE_ID_LENGTH = 1;

  static final char[] EMPTY_CHARS_ARRAY = new char[0];

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
   * @throws IllegalArgumentException if username length is greater than 255
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param username the char sequence which represents user name.
   * @param password the char sequence which represents user password.
   */
  public static ByteBuf encodeSimpleMetadata(
      ByteBufAllocator allocator, char[] username, char[] password) {

    int usernameLength = CharByteBufUtil.utf8Bytes(username);
    if (usernameLength > 255) {
      throw new IllegalArgumentException(
          "Username should be shorter than or equal to 255 bytes length in UTF-8 encoding");
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

  /**
   * Get the first {@code byte} from a {@link ByteBuf} and check whether it is length or {@link
   * WellKnownAuthType}. Assuming said buffer properly contains such a {@code byte}
   *
   * @param metadata byteBuf used to get information from
   */
  public static boolean isWellKnownAuthType(ByteBuf metadata) {
    byte lengthOrId = metadata.getByte(0);
    return (lengthOrId & STREAM_METADATA_LENGTH_MASK) != lengthOrId;
  }

  /**
   * Read first byte from the given {@code metadata} and tries to convert it's value to {@link
   * WellKnownAuthType}.
   *
   * @param metadata given metadata buffer to read from
   * @return Return on of the know Auth types or {@link WellKnownAuthType#UNPARSEABLE_AUTH_TYPE} if
   *     field's value is length or unknown auth type
   * @throws IllegalStateException if not enough readable bytes in the given ByteBuf
   */
  public static WellKnownAuthType decodeWellKnownAuthType(ByteBuf metadata) {
    if (metadata.readableBytes() < 1) {
      throw new IllegalStateException(
          "Unable to decode Well Know Auth type. Not enough readable bytes");
    }
    byte lengthOrId = metadata.readByte();
    int normalizedId = (byte) (lengthOrId & STREAM_METADATA_LENGTH_MASK);

    if (normalizedId != lengthOrId) {
      return WellKnownAuthType.fromIdentifier(normalizedId);
    }

    return WellKnownAuthType.UNPARSEABLE_AUTH_TYPE;
  }

  /**
   * Read up to 129 bytes from the given metadata in order to get the custom Auth Type
   *
   * @param metadata
   * @return
   */
  public static CharSequence decodeCustomAuthType(ByteBuf metadata) {
    if (metadata.readableBytes() < 2) {
      throw new IllegalStateException(
          "Unable to decode custom Auth type. Not enough readable bytes");
    }

    byte encodedLength = metadata.readByte();
    if (encodedLength < 0) {
      throw new IllegalStateException(
          "Unable to decode custom Auth type. Incorrect auth type length");
    }

    // encoded length is realLength - 1 in order to avoid intersection with 0x00 authtype
    int realLength = encodedLength + 1;
    if (metadata.readableBytes() < realLength) {
      throw new IllegalArgumentException(
          "Unable to decode custom Auth type. Malformed length or auth type string");
    }

    return metadata.readCharSequence(realLength, CharsetUtil.US_ASCII);
  }

  /**
   * Read all remaining {@code bytes} from the given {@link ByteBuf} and return sliced
   * representation of a payload
   *
   * @param metadata metadata to get payload from. Please note, the {@code metadata#readIndex}
   *     should be set to the beginning of the payload bytes
   * @return sliced {@link ByteBuf} or {@link Unpooled#EMPTY_BUFFER} if no bytes readable in the
   *     given one
   */
  public static ByteBuf decodePayload(ByteBuf metadata) {
    if (metadata.readableBytes() == 0) {
      return Unpooled.EMPTY_BUFFER;
    }

    return metadata.readSlice(metadata.readableBytes());
  }

  /**
   * Read up to 257 {@code bytes} from the given {@link ByteBuf} where the first byte is username
   * length and the subsequent number of bytes equal to decoded length
   *
   * @param simpleAuthMetadata the given metadata to read username from. Please note, the {@code
   *     simpleAuthMetadata#readIndex} should be set to the username length byte
   * @return sliced {@link ByteBuf} or {@link Unpooled#EMPTY_BUFFER} if username length is zero
   */
  public static ByteBuf decodeUsername(ByteBuf simpleAuthMetadata) {
    short usernameLength = decodeUsernameLength(simpleAuthMetadata);

    if (usernameLength == 0) {
      return Unpooled.EMPTY_BUFFER;
    }

    return simpleAuthMetadata.readSlice(usernameLength);
  }

  /**
   * Read all the remaining {@code byte}s from the given {@link ByteBuf} which represents user's
   * password
   *
   * @param simpleAuthMetadata the given metadata to read password from. Please note, the {@code
   *     simpleAuthMetadata#readIndex} should be set to the beginning of the password bytes
   * @return sliced {@link ByteBuf} or {@link Unpooled#EMPTY_BUFFER} if password length is zero
   */
  public static ByteBuf decodePassword(ByteBuf simpleAuthMetadata) {
    if (simpleAuthMetadata.readableBytes() == 0) {
      return Unpooled.EMPTY_BUFFER;
    }

    return simpleAuthMetadata.readSlice(simpleAuthMetadata.readableBytes());
  }
  /**
   * Read up to 257 {@code bytes} from the given {@link ByteBuf} where the first byte is username
   * length and the subsequent number of bytes equal to decoded length
   *
   * @param simpleAuthMetadata the given metadata to read username from. Please note, the {@code
   *     simpleAuthMetadata#readIndex} should be set to the username length byte
   * @return {@code char[]} which represents UTF-8 username
   */
  public static char[] decodeUsernameAsCharArray(ByteBuf simpleAuthMetadata) {
    short usernameLength = decodeUsernameLength(simpleAuthMetadata);

    if (usernameLength == 0) {
      return EMPTY_CHARS_ARRAY;
    }

    return CharByteBufUtil.readUtf8(simpleAuthMetadata, usernameLength);
  }

  /**
   * Read all the remaining {@code byte}s from the given {@link ByteBuf} which represents user's
   * password
   *
   * @param simpleAuthMetadata the given metadata to read username from. Please note, the {@code
   *     simpleAuthMetadata#readIndex} should be set to the beginning of the password bytes
   * @return {@code char[]} which represents UTF-8 password
   */
  public static char[] decodePasswordAsCharArray(ByteBuf simpleAuthMetadata) {
    if (simpleAuthMetadata.readableBytes() == 0) {
      return EMPTY_CHARS_ARRAY;
    }

    return CharByteBufUtil.readUtf8(simpleAuthMetadata, simpleAuthMetadata.readableBytes());
  }

  /**
   * Read all the remaining {@code bytes} from the given {@link ByteBuf} where the first byte is
   * username length and the subsequent number of bytes equal to decoded length
   *
   * @param bearerAuthMetadata the given metadata to read username from. Please note, the {@code
   *     simpleAuthMetadata#readIndex} should be set to the beginning of the password bytes
   * @return {@code char[]} which represents UTF-8 password
   */
  public static char[] decodeBearerTokenAsCharArray(ByteBuf bearerAuthMetadata) {
    if (bearerAuthMetadata.readableBytes() == 0) {
      return EMPTY_CHARS_ARRAY;
    }

    return CharByteBufUtil.readUtf8(bearerAuthMetadata, bearerAuthMetadata.readableBytes());
  }

  private static short decodeUsernameLength(ByteBuf simpleAuthMetadata) {
    if (simpleAuthMetadata.readableBytes() < 1) {
      throw new IllegalStateException(
          "Unable to decode custom username. Not enough readable bytes");
    }

    short usernameLength = simpleAuthMetadata.readUnsignedByte();

    if (simpleAuthMetadata.readableBytes() < usernameLength) {
      throw new IllegalArgumentException(
          "Unable to decode username. Malformed username length or content");
    }

    return usernameLength;
  }
}
