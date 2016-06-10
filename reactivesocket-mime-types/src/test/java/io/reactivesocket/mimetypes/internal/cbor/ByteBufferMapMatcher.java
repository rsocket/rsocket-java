/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.reactivesocket.mimetypes.internal.cbor;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.mockito.ArgumentMatcher;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

public final class ByteBufferMapMatcher {

    private ByteBufferMapMatcher() {
    }

    public static Matcher<Map<String, ByteBuffer>> mapEqualTo(Map<String, ByteBuffer> toCheck) {
        return new ArgumentMatcher<Map<String, ByteBuffer>>() {

            @Override
            public boolean matches(Object argument) {
                if (argument instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, ByteBuffer> arg = (Map<String, ByteBuffer>) argument;
                    if (arg.size() == toCheck.size()) {
                        for (Entry<String, ByteBuffer> e : arg.entrySet()) {
                            ByteBuffer v = toCheck.get(e.getKey());
                            v.rewind();
                            if (null == v || !e.getValue().equals(v)) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Map Equals " + toCheck);
            }
        };
    }
}
