package io.rsocket.metadata;

/**
 * Enumeration of Well Known Mime Types, as defined in the eponymous extension. Such mime types
 * are used in composite metadata (which can include routing and/or tracing metadata).
 */
public enum WellKnownMimeType {

    APPLICATION_AVRO("application/avro", (byte)0),
    APPLICATION_CBOR("application/cbor", (byte)1),
    APPLICATION_GRAPHQL("application/graphql", (byte)2),
    APPLICATION_GZIP("application/gzip", (byte)3),
    APPLICATION_JAVASCRIPT("application/javascript", (byte)4),
    APPLICATION_JSON("application/json", (byte)5),
    APPLICATION_OCTET_STREAM("application/octet-stream", (byte)6),
    APPLICATION_PDF("application/pdf", (byte)7),
    APPLICATION_THRIFT("application/vnd.apache.thrift.binary", (byte) 8),
    APPLICATION_PROTOBUF("application/vnd.google.protobuf", (byte)9),
    APPLICATION_XML("application/xml", (byte)10),
    APPLICATION_ZIP("application/zip", (byte)11),
    AUDIO_AAC("audio/aac", (byte)12),
    AUDIO_MP3("audio/mp3", (byte)13),
    AUDIO_MP4("audio/mp4", (byte)14),
    AUDIO_MPEG3("audio/mpeg3", (byte)15),
    AUDIO_MPEG("audio/mpeg", (byte)16),
    AUDIO_OGG("audio/ogg", (byte)17),
    AUDIO_OPUS("audio/opus", (byte)18),
    AUDIO_VORBIS("audio/vorbis", (byte)19),
    IMAGE_BMP("image/bmp", (byte)20),
    IMAGE_GIG("image/gif", (byte)21),
    IMAGE_HEIC_SEQUENCE("image/heic-sequence", (byte)22),
    IMAGE_HEIC("image/heic", (byte)23),
    IMAGE_HEIF_SEQUENCE("image/heif-sequence", (byte)24),
    IMAGE_HEIF("image/heif", (byte)25),
    IMAGE_JPEG("image/jpeg", (byte)26),
    IMAGE_PNG("image/png", (byte)27),
    IMAGE_TIFF("image/tiff", (byte)28),
    MULTIPART_MIXED("multipart/mixed", (byte)29),
    TEXT_CSS("text/css", (byte)30),
    TEXT_CSV("text/csv", (byte)31),
    TEXT_HTML("text/html", (byte)32),
    TEXT_PLAIN("text/plain", (byte)33),
    TEXT_XML("text/xml", (byte)34),
    VIDEO_H264("video/H264", (byte)35),
    VIDEO_H265("video/H265", (byte)36),
    VIDEO_VP8("video/VP8", (byte)37),
    //... reserved for future use ...
    MESSAGE_RSOCKET_TRACING_ZIPKIN("message/x.rsocket.tracing-zipkin.v0", (byte)125),
    MESSAGE_RSOCKET_ROUTING("message/x.rsocket.routing.v0", (byte)126),
    MESSAGE_RSOCKET_COMPOSITE_METADATA("message/x.rsocket.composite-metadata.v0", (byte)127);

    private final String str;
    private final byte identifier;

    WellKnownMimeType(String str, byte identifier) {
        if (identifier < 0)  throw new IllegalArgumentException("identifier must be between 0 and 127, inclusive");
        this.str = str;
        this.identifier = identifier;
    }

    /**
     * @return the byte identifier of the mime type, guaranteed to be positive or zero.
     */
    public byte getIdentifier() {
        return identifier;
    }

    /**
     * @return the mime type represented as a {@link String}, which is made of US_ASCII compatible characters only
     */
    public String getMime() {
        return str;
    }

    /**
     * @see #getMime()
     */
    @Override
    public String toString() {
        return str;
    }

    /**
     * Find the {@link WellKnownMimeType} for the given {@link String} representation. If the representation if
     * {@code null} or doesn't match a {@link WellKnownMimeType}, an {@link IllegalArgumentException} is thrown.
     * @param mimeType the looked up mime type
     * @return the {@link WellKnownMimeType}
     * @throws IllegalArgumentException if the requested type is unknown or null
     */
    public static WellKnownMimeType fromMimeType(String mimeType) {
        if (mimeType == null) throw new IllegalArgumentException("type must be non-null");
        for (WellKnownMimeType value : values()) {
            if (mimeType.equals(value.str)) {
                return value;
            }
        }
        throw new IllegalArgumentException("not a WellKnownMimeType: " + mimeType);
    }

    /**
     * Find the {@link WellKnownMimeType} for the given ID (as an int). Valid IDs are defined to be integers between 0
     * and 127, inclusive. However some IDs in that are still only reserved and don't have a type associated yet: this
     * method throws an {@link IllegalStateException} when passing such a ID.
     *
     * @param id the looked up id
     * @return the {@link WellKnownMimeType}
     * @throws IllegalArgumentException if the requested id is out of the specification's range
     * @throws IllegalStateException if the requested id is one that is merely reserved
     */
    public static WellKnownMimeType fromId(int id) {
        if (id < 0 || id > 127) {
            throw new IllegalArgumentException("WellKnownMimeType IDs are between 0 and 127, inclusive");
        }
        for (WellKnownMimeType value : values()) {
            if (value.getIdentifier() == id) return value;
        }
        throw new IllegalStateException(id + " is between 0 and 127 yet no WellKnownMimeType found");
    }
}
