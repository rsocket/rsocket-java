package io.reactivesocket.mimetypes.internal.cbor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import io.reactivesocket.mimetypes.internal.AbstractJacksonCodec;

public class CborCodec extends AbstractJacksonCodec {

    private CborCodec(ObjectMapper mapper) {
        super(mapper);
    }

    /**
     * Creates a {@link CborCodec} with default configurations. Use {@link #create(ObjectMapper)} for custom mapper
     * configurations.
     *
     * @return A new instance of {@link CborCodec} with default mapper configurations.
     */
    public static CborCodec create() {
        ObjectMapper mapper = new ObjectMapper(new CBORFactory());
        configureDefaults(mapper);
        return create(mapper);
    }

    /**
     * Creates a {@link CborCodec} with custom mapper. Use {@link #create()} for default mapper configurations.
     *
     * @return A new instance of {@link CborCodec} with the passed mapper.
     */
    public static CborCodec create(ObjectMapper mapper) {
        return new CborCodec(mapper);
    }
}
