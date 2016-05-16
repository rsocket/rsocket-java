package io.reactivesocket.mimetypes.internal.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivesocket.mimetypes.internal.AbstractJacksonCodec;

public class JsonCodec extends AbstractJacksonCodec {

    private JsonCodec(ObjectMapper mapper) {
        super(mapper);
    }

    /**
     * Creates a {@link JsonCodec} with default configurations. Use {@link #create(ObjectMapper)} for custom mapper
     * configurations.
     *
     * @return A new instance of {@link JsonCodec} with default mapper configurations.
     */
    public static JsonCodec create() {
        ObjectMapper mapper = new ObjectMapper();
        configureDefaults(mapper);
        return create(mapper);
    }

    /**
     * Creates a {@link JsonCodec} with custom mapper. Use {@link #create()} for default mapper configurations.
     *
     * @return A new instance of {@link JsonCodec} with the passed mapper.
     */
    public static JsonCodec create(ObjectMapper mapper) {
        return new JsonCodec(mapper);
    }
}
