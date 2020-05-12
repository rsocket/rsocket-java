package io.rsocket.metadata.security;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.metadata.AuthMetadataCodec;

/** @deprecated in favor of {@link io.rsocket.metadata.AuthMetadataCodec} */
@Deprecated
public class AuthMetadataFlyweight {

  static final int STREAM_METADATA_KNOWN_MASK = 0x80; // 1000 0000

  private AuthMetadataFlyweight() {}

  /**
   * Encode a Authentication CompositeMetadata payload using custom authentication type
   *
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param customAuthType the custom mime type to encode.
   * @param metadata the metadata value to encode.
   * @throws IllegalArgumentException in case of {@code customAuthType} is non US_ASCII string or
   *     empty string or its length is greater than 128 bytes
   */
  public static ByteBuf encodeMetadata(
      ByteBufAllocator allocator, String customAuthType, ByteBuf metadata) {

    return AuthMetadataCodec.encodeMetadata(allocator, customAuthType, metadata);
  }

  /**
   * Encode a Authentication CompositeMetadata payload using custom authentication type
   *
   * @param allocator the {@link ByteBufAllocator} to create intermediate buffers as needed.
   * @param authType the well-known mime type to encode.
   * @param metadata the metadata value to encode.
   * @throws IllegalArgumentException in case of {@code authType} is {@link
   *     WellKnownAuthType#UNPARSEABLE_AUTH_TYPE} or {@link
   *     WellKnownAuthType#UNKNOWN_RESERVED_AUTH_TYPE}
   */
  public static ByteBuf encodeMetadata(
      ByteBufAllocator allocator, WellKnownAuthType authType, ByteBuf metadata) {

    return AuthMetadataCodec.encodeMetadata(allocator, WellKnownAuthType.cast(authType), metadata);
  }

  /**
   * Encode a Authentication CompositeMetadata payload using Simple Authentication format
   *
   * @throws IllegalArgumentException if the username length is greater than 255
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param username the char sequence which represents user name.
   * @param password the char sequence which represents user password.
   */
  public static ByteBuf encodeSimpleMetadata(
      ByteBufAllocator allocator, char[] username, char[] password) {
    return AuthMetadataCodec.encodeSimpleMetadata(allocator, username, password);
  }

  /**
   * Encode a Authentication CompositeMetadata payload using Bearer Authentication format
   *
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param token the char sequence which represents BEARER token.
   */
  public static ByteBuf encodeBearerMetadata(ByteBufAllocator allocator, char[] token) {
    return AuthMetadataCodec.encodeBearerMetadata(allocator, token);
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
    return AuthMetadataCodec.encodeMetadataWithCompression(allocator, authType, metadata);
  }

  /**
   * Get the first {@code byte} from a {@link ByteBuf} and check whether it is length or {@link
   * WellKnownAuthType}. Assuming said buffer properly contains such a {@code byte}
   *
   * @param metadata byteBuf used to get information from
   */
  public static boolean isWellKnownAuthType(ByteBuf metadata) {
    return AuthMetadataCodec.isWellKnownAuthType(metadata);
  }

  /**
   * Read first byte from the given {@code metadata} and tries to convert it's value to {@link
   * WellKnownAuthType}.
   *
   * @param metadata given metadata buffer to read from
   * @return Return on of the know Auth types or {@link WellKnownAuthType#UNPARSEABLE_AUTH_TYPE} if
   *     field's value is length or unknown auth type
   * @throws IllegalStateException if not enough readable bytes in the given {@link ByteBuf}
   */
  public static WellKnownAuthType decodeWellKnownAuthType(ByteBuf metadata) {
    return WellKnownAuthType.cast(AuthMetadataCodec.readWellKnownAuthType(metadata));
  }

  /**
   * Read up to 129 bytes from the given metadata in order to get the custom Auth Type
   *
   * @param metadata
   * @return
   */
  public static CharSequence decodeCustomAuthType(ByteBuf metadata) {
    return AuthMetadataCodec.readCustomAuthType(metadata);
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
    return AuthMetadataCodec.readPayload(metadata);
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
    return AuthMetadataCodec.readUsername(simpleAuthMetadata);
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
    return AuthMetadataCodec.readPassword(simpleAuthMetadata);
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
    return AuthMetadataCodec.readUsernameAsCharArray(simpleAuthMetadata);
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
    return AuthMetadataCodec.readPasswordAsCharArray(simpleAuthMetadata);
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
    return AuthMetadataCodec.readBearerTokenAsCharArray(bearerAuthMetadata);
  }
}
