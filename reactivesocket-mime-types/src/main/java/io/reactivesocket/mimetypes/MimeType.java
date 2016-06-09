package io.reactivesocket.mimetypes;

import io.reactivesocket.Frame;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;

/**
 * Encoding and decoding operations for a ReactiveSocket. Since, mime-types for data and metadata do not change once
 * setup, a MimeType instance can be stored per ReactiveSocket instance and can be used for repeated encode/decode of
 * data and metadata.
 */
public interface MimeType {

    /**
     * Decodes metadata of the passed frame to the specified {@code clazz}.
     *
     * @param toDecode Frame for which metadata is to be decoded.
     * @param clazz Class to which metadata will be decoded.
     *
     * @param <T> Type of the class to which metadata will be decoded.
     *
     * @return Instance of the class post decode.
     */
    default <T> T decodeMetadata(Frame toDecode, Class<T> clazz) {
        return decodeMetadata(toDecode.getMetadata(), clazz);
    }

    /**
     * Decodes the passed buffer to the specified {@code clazz}.
     *
     * @param toDecode buffer to be decoded.
     * @param clazz Class to which the buffer will be decoded.
     *
     * @param <T> Type of the class to which the buffer will be decoded.
     *
     * @return Instance of the class post decode.
     */
    <T> T decodeMetadata(ByteBuffer toDecode, Class<T> clazz);

    /**
     * Decodes the passed buffer to the specified {@code clazz}.
     *
     * @param <T> Type of the class to which the buffer will be decoded.
     *
     * @param toDecode buffer to be decoded.
     * @param clazz Class to which the buffer will be decoded.
     * @param offset Offset in the buffer.
     *
     * @return Instance of the class post decode.
     */
    <T> T decodeMetadata(DirectBuffer toDecode, Class<T> clazz, int offset);

    /**
     * Encodes passed metadata to a buffer.
     *
     * @param toEncode Object to encode as metadata.
     *
     * @param <T> Type of the object to encode.
     *
     * @return Buffer with encoded data.
     */
    <T> ByteBuffer encodeMetadata(T toEncode);

    /**
     * Encodes passed metadata to a buffer.
     *
     * @param toEncode Object to encode as metadata.
     *
     * @param <T> Type of the object to encode.
     *
     * @return Buffer with encoded data.
     */
    <T> DirectBuffer encodeMetadataDirect(T toEncode);

    /**
     * Encodes passed metadata to the passed buffer.
     *
     * @param <T> Type of the object to encode.
     * @param buffer Encodes the metadata to this buffer.
     * @param toEncode Metadata to encode.
     * @param offset Offset in the buffer to start writing.
     */
    <T> void encodeMetadataTo(MutableDirectBuffer buffer, T toEncode, int offset);

    /**
     * Encodes passed metadata to the passed buffer.
     *
     * @param buffer Encodes the metadata to this buffer.
     * @param toEncode Metadata to encode.
     *
     * @param <T> Type of the object to encode.
     */
    <T> void encodeMetadataTo(ByteBuffer buffer, T toEncode);

    /**
     * Decodes data of the passed frame to the specified {@code clazz}.
     *
     * @param toDecode Frame for which metadata is to be decoded.
     * @param clazz Class to which metadata will be decoded.
     *
     * @param <T> Type of the class to which metadata will be decoded.
     *
     * @return Instance of the class post decode.
     */
    default <T> T decodeData(Frame toDecode, Class<T> clazz) {
        return decodeData(toDecode.getData(), clazz);
    }

    /**
     * Decodes the passed buffer to the specified {@code clazz}.
     *
     * @param toDecode buffer to be decoded.
     * @param clazz Class to which the buffer will be decoded.
     *
     * @param <T> Type of the class to which the buffer will be decoded.
     *
     * @return Instance of the class post decode.
     */
    <T> T decodeData(ByteBuffer toDecode, Class<T> clazz);

    /**
     * Decodes the passed buffer to the specified {@code clazz}.
     *
     * @param <T> Type of the class to which the buffer will be decoded.
     *
     * @param toDecode buffer to be decoded.
     * @param clazz Class to which the buffer will be decoded.
     * @param offset Offset in the buffer.
     *
     * @return Instance of the class post decode.
     */
    <T> T decodeData(DirectBuffer toDecode, Class<T> clazz, int offset);

    /**
     * Encodes passed data to a buffer.
     *
     * @param toEncode Object to encode as data.
     *
     * @param <T> Type of the object to encode.
     *
     * @return Buffer with encoded data.
     */
    <T> ByteBuffer encodeData(T toEncode);

    /**
     * Encodes passed data to a buffer.
     *
     * @param toEncode Object to encode as data.
     *
     * @param <T> Type of the object to encode.
     *
     * @return Buffer with encoded data.
     */
    <T> DirectBuffer encodeDataDirect(T toEncode);

    /**
     * Encodes passed data to the passed buffer.
     *
     * @param <T> Type of the object to encode.
     * @param buffer Encodes the data to this buffer.
     * @param toEncode Data to encode.
     * @param offset Offset in the buffer to start writing.
     */
    <T> void encodeDataTo(MutableDirectBuffer buffer, T toEncode, int offset);

    /**
     * Encodes passed data to the passed buffer.
     *
     * @param buffer Encodes the data to this buffer.
     * @param toEncode Data to encode.
     *
     * @param <T> Type of the object to encode.
     */
    <T> void encodeDataTo(ByteBuffer buffer, T toEncode);
}
