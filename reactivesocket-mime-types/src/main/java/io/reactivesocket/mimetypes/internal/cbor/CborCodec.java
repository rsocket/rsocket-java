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
