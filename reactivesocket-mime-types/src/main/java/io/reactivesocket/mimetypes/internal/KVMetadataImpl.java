package io.reactivesocket.mimetypes.internal;

import io.reactivesocket.mimetypes.KVMetadata;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class KVMetadataImpl implements KVMetadata {

    private Map<String, ByteBuffer> store;

    public KVMetadataImpl(Map<String, ByteBuffer> store) {
        this.store = store;
    }

    public KVMetadataImpl() {
        store = new HashMap<>();
    }

    public void setStore(Map<String, ByteBuffer> store) {
        if (null == store) {
            throw new IllegalArgumentException("Store can not be null");
        }
        this.store = store;
    }

    public Map<String, ByteBuffer> getStore() {
        return store;
    }

    @Override
    public String getAsString(String key, Charset valueEncoding) {
        ByteBuffer toReturn = get(key);

        if (null != toReturn) {
            byte[] dst = new byte[toReturn.remaining()];
            toReturn.get(dst);
            return new String(dst, valueEncoding);
        }

        return null;
    }

    @Override
    public KVMetadata duplicate(Function<Integer, MutableDirectBuffer> newBufferFactory) {
        Map<String, ByteBuffer> copy = new HashMap<>(store);
        return new KVMetadataImpl(copy);
    }

    @Override
    public int size() {
        return store.size();
    }

    @Override
    public boolean isEmpty() {
        return store.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return store.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return store.containsValue(value);
    }

    @Override
    public ByteBuffer get(Object key) {
        return store.get(key);
    }

    @Override
    public ByteBuffer put(String key, ByteBuffer value) {
        return store.put(key, value);
    }

    @Override
    public ByteBuffer remove(Object key) {
        return store.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends ByteBuffer> m) {
        store.putAll(m);
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public Set<String> keySet() {
        return store.keySet();
    }

    @Override
    public Collection<ByteBuffer> values() {
        return store.values();
    }

    @Override
    public Set<Entry<String, ByteBuffer>> entrySet() {
        return store.entrySet();
    }

    @Override
    public String toString() {
        return  "KVMetadataImpl{" + "store=" + store + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KVMetadataImpl)) {
            return false;
        }

        KVMetadataImpl that = (KVMetadataImpl) o;

        if (!store.equals(that.store)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return store.hashCode();
    }
}
