package io.reactivesocket.mimetypes.internal;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.io.DirectBufferInputStream;
import org.agrona.io.MutableDirectBufferOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class AbstractJacksonCodec implements Codec {

    private static final ThreadLocal<DirectBufferInputStream> directInWrappers =
            ThreadLocal.withInitial(DirectBufferInputStream::new);

    private static final ThreadLocal<MutableDirectBufferOutputStream> directOutWrappers =
            ThreadLocal.withInitial(MutableDirectBufferOutputStream::new);

    private static final ThreadLocal<ByteBufferInputStream> bbInWrappers =
            ThreadLocal.withInitial(ByteBufferInputStream::new);

    private static final ThreadLocal<ByteBufferOutputStream> bbOutWrappers =
            ThreadLocal.withInitial(ByteBufferOutputStream::new);

    private static final byte[] emptyByteArray = new byte[0];
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private static final DirectBuffer EMPTY_DIRECT_BUFFER = new UnsafeBuffer(emptyByteArray);

    private final ObjectMapper mapper;

    protected AbstractJacksonCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    protected static void configureDefaults(ObjectMapper mapper) {
        mapper.registerModule(new AfterburnerModule());
        mapper.configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.enable(Feature.AUTO_CLOSE_TARGET); // encodeTo methods do not close the OutputStream.
        SimpleModule module = new SimpleModule(Version.unknownVersion());
        mapper.registerModule(module);
    }

    @Override
    public <T> T decode(ByteBuffer buffer, Class<T> tClass) {
        return _decode(bbInWrappers.get().wrap(buffer), tClass);
    }

    @Override
    public <T> T decode(DirectBuffer buffer, int offset, Class<T> tClass) {
        DirectBufferInputStream stream = directInWrappers.get();
        stream.wrap(buffer, offset, buffer.capacity());
        return _decode(stream, tClass);
    }

    @Override
    public <T> ByteBuffer encode(T toEncode) {
        byte[] bytes = _encode(toEncode);
        return bytes == emptyByteArray ? EMPTY_BUFFER : ByteBuffer.wrap(bytes);
    }

    @Override
    public <T> DirectBuffer encodeDirect(T toEncode) {
        byte[] bytes = _encode(toEncode);
        return bytes == emptyByteArray ? EMPTY_DIRECT_BUFFER : new UnsafeBuffer(bytes);
    }

    @Override
    public <T> void encodeTo(ByteBuffer buffer, T toEncode) {
        _encodeTo(bbOutWrappers.get().wrap(buffer), toEncode);
    }

    @Override
    public <T> void encodeTo(MutableDirectBuffer buffer, T toEncode, int offset) {
        MutableDirectBufferOutputStream stream = directOutWrappers.get();
        stream.wrap(buffer, offset, buffer.capacity());
        _encodeTo(stream, toEncode);
    }

    private <T> T _decode(InputStream stream, Class<T> clazz) {
        T v = null;
        try {
            v = mapper.readValue(stream, clazz);
        } catch (IOException e) {
            LangUtil.rethrowUnchecked(e);
        }

        return v;
    }

    private byte[] _encode(Object toEncode) {
        byte[] encode = emptyByteArray;

        try {
            encode = mapper.writeValueAsBytes(toEncode);
        } catch (JsonProcessingException e) {
            LangUtil.rethrowUnchecked(e);
        }

        return encode;
    }

    private void  _encodeTo(OutputStream stream, Object toEncode) {
        try {
            mapper.writeValue(stream, toEncode);
        } catch (JsonProcessingException e) {
            LangUtil.rethrowUnchecked(e);
        } catch (IOException e) {
            LangUtil.rethrowUnchecked(e);
        }
    }
}
