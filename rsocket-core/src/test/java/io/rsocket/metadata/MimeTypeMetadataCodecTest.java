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
package io.rsocket.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MimeTypeMetadataCodec}. */
public class MimeTypeMetadataCodecTest {

  @Test
  public void wellKnownMimeType() {
    WellKnownMimeType mimeType = WellKnownMimeType.APPLICATION_HESSIAN;
    ByteBuf byteBuf = MimeTypeMetadataCodec.encode(ByteBufAllocator.DEFAULT, mimeType);
    try {
      List<String> mimeTypes = MimeTypeMetadataCodec.decode(byteBuf);

      assertThat(mimeTypes.size()).isEqualTo(1);
      assertThat(WellKnownMimeType.fromString(mimeTypes.get(0))).isEqualTo(mimeType);
    } finally {
      byteBuf.release();
    }
  }

  @Test
  public void customMimeType() {
    String mimeType = "aaa/bb";
    ByteBuf byteBuf = MimeTypeMetadataCodec.encode(ByteBufAllocator.DEFAULT, mimeType);
    try {
      List<String> mimeTypes = MimeTypeMetadataCodec.decode(byteBuf);

      assertThat(mimeTypes.size()).isEqualTo(1);
      assertThat(mimeTypes.get(0)).isEqualTo(mimeType);
    } finally {
      byteBuf.release();
    }
  }

  @Test
  public void multipleMimeTypes() {
    List<String> mimeTypes = Lists.newArrayList("aaa/bbb", "application/x-hessian");
    ByteBuf byteBuf = MimeTypeMetadataCodec.encode(ByteBufAllocator.DEFAULT, mimeTypes);

    try {
      assertThat(MimeTypeMetadataCodec.decode(byteBuf)).isEqualTo(mimeTypes);
    } finally {
      byteBuf.release();
    }
  }
}
