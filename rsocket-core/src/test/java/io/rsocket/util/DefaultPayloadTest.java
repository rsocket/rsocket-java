/*
 * Copyright 2015-2018 the original author or authors.
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
 */

package io.rsocket.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import java.nio.ByteBuffer;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class DefaultPayloadTest {
  public static final String DATA_VAL = "data";
  public static final String METADATA_VAL = "metadata";

  @Test
  public void testReuse() {
    Payload p = DefaultPayload.create(DATA_VAL, METADATA_VAL);
    assertDataAndMetadata(p, DATA_VAL, METADATA_VAL);
    assertDataAndMetadata(p, DATA_VAL, METADATA_VAL);
  }

  public void assertDataAndMetadata(Payload p, String dataVal, String metadataVal) {
    assertThat("Unexpected data.", p.getDataUtf8(), equalTo(dataVal));
    if (metadataVal == null) {
      assertThat("Non-null metadata", p.hasMetadata(), equalTo(false));
    } else {
      assertThat("Null metadata", p.hasMetadata(), equalTo(true));
      assertThat("Unexpected metadata.", p.getMetadataUtf8(), equalTo(metadataVal));
    }
  }

  @Test
  public void staticMethods() {
    assertDataAndMetadata(DefaultPayload.create(DATA_VAL, METADATA_VAL), DATA_VAL, METADATA_VAL);
    assertDataAndMetadata(DefaultPayload.create(DATA_VAL), DATA_VAL, null);
  }

  @Test
  public void shouldIndicateThatItHasNotMetadata() {
    Payload payload = DefaultPayload.create("data");

    Assertions.assertThat(payload.hasMetadata()).isFalse();
  }

  @Test
  public void shouldIndicateThatItHasNotMetadata1() {
    Payload payload =
        DefaultPayload.create(Unpooled.wrappedBuffer("data".getBytes()), Unpooled.EMPTY_BUFFER);

    Assertions.assertThat(payload.hasMetadata()).isFalse();
  }

  @Test
  public void shouldIndicateThatItHasNotMetadata2() {
    Payload payload =
        DefaultPayload.create(ByteBuffer.wrap("data".getBytes()), ByteBuffer.allocate(0));

    Assertions.assertThat(payload.hasMetadata()).isFalse();
  }
}
