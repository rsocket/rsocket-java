package io.reactivesocket.mimetypes;

import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.function.Function;

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
     * @throws NullPointerException If the key does not exist.
     */
    String getAsString(String key, Charset valueEncoding);

    /**
     * Creates a new copy of this metadata.
     *
     * @param newBufferFactory A factory to create new buffer instances to copy, if required. The argument to the
     * function is the capacity of the new buffer.
     *
     * @return New copy of this metadata.
     */
    KVMetadata duplicate(Function<Integer, MutableDirectBuffer> newBufferFactory);
}
