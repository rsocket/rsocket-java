/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
