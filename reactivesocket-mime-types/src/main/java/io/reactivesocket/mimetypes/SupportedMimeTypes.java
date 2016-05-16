package io.reactivesocket.mimetypes;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum SupportedMimeTypes {

    /*CBOR encoding*/
    CBOR ("application/cbor"),
    /*JSON encoding*/
    JSON ("application/json"),
    /*Default ReactiveSocket metadata encoding as specified in
    https://github.com/ReactiveSocket/reactivesocket/blob/mimetypes/MimeTypes.md*/
    ReactiveSocketDefaultMetadata ("application/x.reactivesocket.meta+cbor");

    private final List<String> mimeTypes;

    SupportedMimeTypes(String... mimeTypes) {
        this.mimeTypes = Collections.unmodifiableList(Arrays.asList(mimeTypes));
    }

    /**
     * Parses the passed string to this enum.
     *
     * @param mimeType Mimetype to parse.
     *
     * @return This enum if the mime type is supported, else {@code null}
     */
    public static SupportedMimeTypes parse(String mimeType) {
        for (SupportedMimeTypes aMimeType : SupportedMimeTypes.values()) {
            if (aMimeType.mimeTypes.contains(mimeType)) {
                return aMimeType;
            }
        }
        return null;
    }

    /**
     * Same as {@link #parse(String)} but throws an exception if the passed mime type is not supported.
     *
     * @param mimeType Mime-type to parse.
     *
     * @return This enum instance.
     *
     * @throws IllegalArgumentException If the mime-type is not supported.
     */
    public static SupportedMimeTypes parseOrDie(String mimeType) {
        SupportedMimeTypes parsed = parse(mimeType);
        if (null == parsed) {
            throw new IllegalArgumentException("Unsupported mime-type: " + mimeType);
        }
        return parsed;
    }

    public List<String> getMimeTypes() {
        return mimeTypes;
    }
}
