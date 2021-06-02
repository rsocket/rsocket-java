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

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

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
    assertThat(p.getDataUtf8()).describedAs("Unexpected data.").isEqualTo(dataVal);
    if (metadataVal == null) {
      assertThat(p.hasMetadata()).describedAs("Non-null metadata").isEqualTo(false);
    } else {
      assertThat(p.hasMetadata()).describedAs("Null metadata").isEqualTo(true);
      assertThat(p.getMetadataUtf8()).describedAs("Unexpected metadata.").isEqualTo(metadataVal);
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

    assertThat(payload.hasMetadata()).isFalse();
  }

  @Test
  public void shouldIndicateThatItHasMetadata1() {
    Payload payload =
        DefaultPayload.create(Unpooled.wrappedBuffer("data".getBytes()), Unpooled.EMPTY_BUFFER);

    assertThat(payload.hasMetadata()).isTrue();
  }

  @Test
  public void shouldIndicateThatItHasMetadata2() {
    Payload payload =
        DefaultPayload.create(ByteBuffer.wrap("data".getBytes()), ByteBuffer.allocate(0));

    assertThat(payload.hasMetadata()).isTrue();
  }

  @Test
  public void shouldReleaseGivenByteBufDataAndMetadataUpOnPayloadCreation() {
    LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    for (byte i = 0; i < 126; i++) {
      ByteBuf data = allocator.buffer();
      data.writeByte(i);

      boolean metadataPresent = ThreadLocalRandom.current().nextBoolean();
      ByteBuf metadata = null;
      if (metadataPresent) {
        metadata = allocator.buffer();
        metadata.writeByte(i + 1);
      }

      Payload payload = DefaultPayload.create(data, metadata);

      assertThat(payload.getData()).isEqualTo(ByteBuffer.wrap(new byte[] {i}));

      assertThat(payload.getMetadata())
          .isEqualTo(
              metadataPresent
                  ? ByteBuffer.wrap(new byte[] {(byte) (i + 1)})
                  : DefaultPayload.EMPTY_BUFFER);
      allocator.assertHasNoLeaks();
    }
  }
}
