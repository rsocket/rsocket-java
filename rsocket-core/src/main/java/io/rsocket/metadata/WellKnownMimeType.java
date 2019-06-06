package io.rsocket.metadata;

import java.util.Arrays;

/**
 * Enumeration of Well Known Mime Types, as defined in the eponymous extension. Such mime types are
 * used in composite metadata (which can include routing and/or tracing metadata). Per
 * specification, identifiers are between 0 and 127 (inclusive).
 */
public enum WellKnownMimeType {
  UNPARSEABLE_MIME_TYPE("UNPARSEABLE_MIME_TYPE_DO_NOT_USE", (byte) -2),
  UNKNOWN_RESERVED_MIME_TYPE("UNKNOWN_YET_RESERVED_DO_NOT_USE", (byte) -1),
  APPLICATION_AVRO("application/avro", (byte) 0),
  APPLICATION_CBOR("application/cbor", (byte) 1),
  APPLICATION_GRAPHQL("application/graphql", (byte) 2),
  APPLICATION_GZIP("application/gzip", (byte) 3),
  APPLICATION_JAVASCRIPT("application/javascript", (byte) 4),
  APPLICATION_JSON("application/json", (byte) 5),
  APPLICATION_OCTET_STREAM("application/octet-stream", (byte) 6),
  APPLICATION_PDF("application/pdf", (byte) 7),
  APPLICATION_THRIFT("application/vnd.apache.thrift.binary", (byte) 8),
  APPLICATION_PROTOBUF("application/vnd.google.protobuf", (byte) 9),
  APPLICATION_XML("application/xml", (byte) 10),
  APPLICATION_ZIP("application/zip", (byte) 11),
  AUDIO_AAC("audio/aac", (byte) 12),
  AUDIO_MP3("audio/mp3", (byte) 13),
  AUDIO_MP4("audio/mp4", (byte) 14),
  AUDIO_MPEG3("audio/mpeg3", (byte) 15),
  AUDIO_MPEG("audio/mpeg", (byte) 16),
  AUDIO_OGG("audio/ogg", (byte) 17),
  AUDIO_OPUS("audio/opus", (byte) 18),
  AUDIO_VORBIS("audio/vorbis", (byte) 19),
  IMAGE_BMP("image/bmp", (byte) 20),
  IMAGE_GIG("image/gif", (byte) 21),
  IMAGE_HEIC_SEQUENCE("image/heic-sequence", (byte) 22),
  IMAGE_HEIC("image/heic", (byte) 23),
  IMAGE_HEIF_SEQUENCE("image/heif-sequence", (byte) 24),
  IMAGE_HEIF("image/heif", (byte) 25),
  IMAGE_JPEG("image/jpeg", (byte) 26),
  IMAGE_PNG("image/png", (byte) 27),
  IMAGE_TIFF("image/tiff", (byte) 28),
  MULTIPART_MIXED("multipart/mixed", (byte) 29),
  TEXT_CSS("text/css", (byte) 30),
  TEXT_CSV("text/csv", (byte) 31),
  TEXT_HTML("text/html", (byte) 32),
  TEXT_PLAIN("text/plain", (byte) 33),
  TEXT_XML("text/xml", (byte) 34),
  VIDEO_H264("video/H264", (byte) 35),
  VIDEO_H265("video/H265", (byte) 36),
  VIDEO_VP8("video/VP8", (byte) 37),
  // ... reserved for future use ...
  MESSAGE_RSOCKET_TRACING_ZIPKIN("message/x.rsocket.tracing-zipkin.v0", (byte) 125),
  MESSAGE_RSOCKET_ROUTING("message/x.rsocket.routing.v0", (byte) 126),
  MESSAGE_RSOCKET_COMPOSITE_METADATA("message/x.rsocket.composite-metadata.v0", (byte) 127);

  private static final WellKnownMimeType[] TYPES_BY_MIME_ID;

  static {
    // precompute an array of all valid mime ids, filling the blanks with the RESERVED enum
    TYPES_BY_MIME_ID = new WellKnownMimeType[128]; // 0-127 inclusive
    Arrays.fill(TYPES_BY_MIME_ID, UNKNOWN_RESERVED_MIME_TYPE);

    for (WellKnownMimeType value : values()) {
      if (value.getIdentifier() >= 0) {
        TYPES_BY_MIME_ID[value.getIdentifier()] = value;
      }
    }
  }

  private final String str;
  private final byte identifier;

  WellKnownMimeType(String str, byte identifier) {
    this.str = str;
    this.identifier = identifier;
  }

  /** @return the byte identifier of the mime type, guaranteed to be positive or zero. */
  public byte getIdentifier() {
    return identifier;
  }

  /**
   * @return the mime type represented as a {@link String}, which is made of US_ASCII compatible
   *     characters only
   */
  public String getMime() {
    return str;
  }

  /** @see #getMime() */
  @Override
  public String toString() {
    return str;
  }

  /**
   * Find the {@link WellKnownMimeType} for the given {@link String} representation. If the
   * representation is {@code null} or doesn't match a {@link WellKnownMimeType}, the {@link
   * #UNPARSEABLE_MIME_TYPE} is returned.
   *
   * @param mimeType the looked up mime type
   * @return the matching {@link WellKnownMimeType}, or {@link #UNPARSEABLE_MIME_TYPE} if none
   *     matches
   */
  public static WellKnownMimeType fromMimeType(String mimeType) {
    if (mimeType == null) throw new IllegalArgumentException("type must be non-null");

    // force UNPARSEABLE if by chance UNKNOWN_RESERVED_MIME_TYPE's text has been used
    if (mimeType.equals(UNKNOWN_RESERVED_MIME_TYPE.str)) {
      return UNPARSEABLE_MIME_TYPE;
    }

    for (WellKnownMimeType value : values()) {
      if (mimeType.equals(value.str)) {
        return value;
      }
    }
    return UNPARSEABLE_MIME_TYPE;
  }

  /**
   * Find the {@link WellKnownMimeType} for the given ID (as an int). Valid IDs are defined to be
   * integers between 0 and 127, inclusive. IDs outside of this range will produce the {@link
   * #UNPARSEABLE_MIME_TYPE}. Additionally, some IDs in that range are still only reserved and don't
   * have a type associated yet: this method returns the {@link #UNKNOWN_RESERVED_MIME_TYPE} when
   * passing such a ID, which lets call sites potentially detect this and keep the original
   * representation when transmitting the associated metadata buffer.
   *
   * @param id the looked up id
   * @return the {@link WellKnownMimeType}, or {@link #UNKNOWN_RESERVED_MIME_TYPE} if the id is out
   *     of the specification's range, or {@link #UNKNOWN_RESERVED_MIME_TYPE} if the id is one that
   *     is merely reserved but unknown to this implementation.
   */
  public static WellKnownMimeType fromId(int id) {
    if (id < 0 || id > 127) {
      return UNPARSEABLE_MIME_TYPE;
    }
    return TYPES_BY_MIME_ID[id];
  }
}
