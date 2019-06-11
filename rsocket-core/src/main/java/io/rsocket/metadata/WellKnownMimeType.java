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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration of Well Known Mime Types, as defined in the eponymous extension. Such mime types are
 * used in composite metadata (which can include routing and/or tracing metadata). Per
 * specification, identifiers are between 0 and 127 (inclusive).
 */
public enum WellKnownMimeType {
  UNPARSEABLE_MIME_TYPE("UNPARSEABLE_MIME_TYPE_DO_NOT_USE", (byte) -2),
  UNKNOWN_RESERVED_MIME_TYPE("UNKNOWN_YET_RESERVED_DO_NOT_USE", (byte) -1),

  APPLICATION_AVRO("application/avro", (byte) 0x00),
  APPLICATION_CBOR("application/cbor", (byte) 0x01),
  APPLICATION_GRAPHQL("application/graphql", (byte) 0x02),
  APPLICATION_GZIP("application/gzip", (byte) 0x03),
  APPLICATION_JAVASCRIPT("application/javascript", (byte) 0x04),
  APPLICATION_JSON("application/json", (byte) 0x05),
  APPLICATION_OCTET_STREAM("application/octet-stream", (byte) 0x06),
  APPLICATION_PDF("application/pdf", (byte) 0x07),
  APPLICATION_THRIFT("application/vnd.apache.thrift.binary", (byte) 0x08),
  APPLICATION_PROTOBUF("application/vnd.google.protobuf", (byte) 0x09),
  APPLICATION_XML("application/xml", (byte) 0x0A),
  APPLICATION_ZIP("application/zip", (byte) 0x0B),
  AUDIO_AAC("audio/aac", (byte) 0x0C),
  AUDIO_MP3("audio/mp3", (byte) 0x0D),
  AUDIO_MP4("audio/mp4", (byte) 0x0E),
  AUDIO_MPEG3("audio/mpeg3", (byte) 0x0F),
  AUDIO_MPEG("audio/mpeg", (byte) 0x10),
  AUDIO_OGG("audio/ogg", (byte) 0x11),
  AUDIO_OPUS("audio/opus", (byte) 0x12),
  AUDIO_VORBIS("audio/vorbis", (byte) 0x13),
  IMAGE_BMP("image/bmp", (byte) 0x14),
  IMAGE_GIG("image/gif", (byte) 0x15),
  IMAGE_HEIC_SEQUENCE("image/heic-sequence", (byte) 0x16),
  IMAGE_HEIC("image/heic", (byte) 0x17),
  IMAGE_HEIF_SEQUENCE("image/heif-sequence", (byte) 0x18),
  IMAGE_HEIF("image/heif", (byte) 0x19),
  IMAGE_JPEG("image/jpeg", (byte) 0x1A),
  IMAGE_PNG("image/png", (byte) 0x1B),
  IMAGE_TIFF("image/tiff", (byte) 0x1C),
  MULTIPART_MIXED("multipart/mixed", (byte) 0x1D),
  TEXT_CSS("text/css", (byte) 0x1E),
  TEXT_CSV("text/csv", (byte) 0x1F),
  TEXT_HTML("text/html", (byte) 0x20),
  TEXT_PLAIN("text/plain", (byte) 0x21),
  TEXT_XML("text/xml", (byte) 0x22),
  VIDEO_H264("video/H264", (byte) 0x23),
  VIDEO_H265("video/H265", (byte) 0x24),
  VIDEO_VP8("video/VP8", (byte) 0x25),

  // ... reserved for future use ...

  MESSAGE_RSOCKET_TRACING_ZIPKIN("message/x.rsocket.tracing-zipkin.v0", (byte) 0x7D),
  MESSAGE_RSOCKET_ROUTING("message/x.rsocket.routing.v0", (byte) 0x7E),
  MESSAGE_RSOCKET_COMPOSITE_METADATA("message/x.rsocket.composite-metadata.v0", (byte) 0x7F);

  static final WellKnownMimeType[] TYPES_BY_MIME_ID;
  static final Map<String, WellKnownMimeType> TYPES_BY_MIME_STRING;

  static {
    // precompute an array of all valid mime ids, filling the blanks with the RESERVED enum
    TYPES_BY_MIME_ID = new WellKnownMimeType[128]; // 0-127 inclusive
    Arrays.fill(TYPES_BY_MIME_ID, UNKNOWN_RESERVED_MIME_TYPE);
    // also prepare a Map of the types by mime string
    TYPES_BY_MIME_STRING = new HashMap<>(128);

    for (WellKnownMimeType value : values()) {
      if (value.getIdentifier() >= 0) {
        TYPES_BY_MIME_ID[value.getIdentifier()] = value;
        TYPES_BY_MIME_STRING.put(value.getString(), value);
      }
    }
  }

  private final byte identifier;
  private final String str;

  WellKnownMimeType(String str, byte identifier) {
    this.str = str;
    this.identifier = identifier;
  }

  /**
   * Find the {@link WellKnownMimeType} for the given identifier (as an {@code int}). Valid
   * identifiers are defined to be integers between 0 and 127, inclusive. Identifiers outside of
   * this range will produce the {@link #UNPARSEABLE_MIME_TYPE}. Additionally, some identifiers in
   * that range are still only reserved and don't have a type associated yet: this method returns
   * the {@link #UNKNOWN_RESERVED_MIME_TYPE} when passing such an identifier, which lets call sites
   * potentially detect this and keep the original representation when transmitting the associated
   * metadata buffer.
   *
   * @param id the looked up identifier
   * @return the {@link WellKnownMimeType}, or {@link #UNKNOWN_RESERVED_MIME_TYPE} if the id is out
   *     of the specification's range, or {@link #UNKNOWN_RESERVED_MIME_TYPE} if the id is one that
   *     is merely reserved but unknown to this implementation.
   */
  public static WellKnownMimeType fromIdentifier(int id) {
    if (id < 0x00 || id > 0x7F) {
      return UNPARSEABLE_MIME_TYPE;
    }
    return TYPES_BY_MIME_ID[id];
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
  public static WellKnownMimeType fromString(String mimeType) {
    if (mimeType == null) throw new IllegalArgumentException("type must be non-null");

    // force UNPARSEABLE if by chance UNKNOWN_RESERVED_MIME_TYPE's text has been used
    if (mimeType.equals(UNKNOWN_RESERVED_MIME_TYPE.str)) {
      return UNPARSEABLE_MIME_TYPE;
    }

    return TYPES_BY_MIME_STRING.getOrDefault(mimeType, UNPARSEABLE_MIME_TYPE);
  }

  /** @return the byte identifier of the mime type, guaranteed to be positive or zero. */
  public byte getIdentifier() {
    return identifier;
  }

  /**
   * @return the mime type represented as a {@link String}, which is made of US_ASCII compatible
   *     characters only
   */
  public String getString() {
    return str;
  }

  /** @see #getString() */
  @Override
  public String toString() {
    return str;
  }
}
