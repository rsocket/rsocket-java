package io.reactivesocket.mimetypes;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * A representation of ReactiveSocket metadata as a key-value pair.
 *
 * <b>Implementations are not required to be thread-safe.</b>
 */
public interface KVMetadata extends Map<String, ByteBuffer> {

    /**
     * Lookup the value for the passed key and return the value as a string.
     *
     * @param key To Lookup.
     * @param valueEncoding Encoding for the value.
     *
     * @return Value as a string with the passed {@code valueEncoding}
     */
    String getAsString(String key, Charset valueEncoding);

}
