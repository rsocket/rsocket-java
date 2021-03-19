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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.Test;

public class MimeMetadataCodecTest {

  @Test
  public void customMimeType() {
    String customMimeType = "aaa/bb";
    ByteBuf byteBuf = MimeTypeMetadataCodec.encode(ByteBufAllocator.DEFAULT, customMimeType);
    List<String> mimeTypes = MimeTypeMetadataCodec.decode(byteBuf);
    Assertions.assertThat(mimeTypes.size()).isEqualTo(1);
    Assertions.assertThat(customMimeType).isEqualTo(mimeTypes.get(0));
  }

  @Test
  public void wellKnowMimeType() {
    WellKnownMimeType wellKnownMimeType = WellKnownMimeType.APPLICATION_HESSIAN;
    ByteBuf byteBuf = MimeTypeMetadataCodec.encode(ByteBufAllocator.DEFAULT, wellKnownMimeType);
    List<String> mimes = MimeTypeMetadataCodec.decode(byteBuf);
    Assertions.assertThat(mimes.size()).isEqualTo(1);
    Assertions.assertThat(wellKnownMimeType).isEqualTo(WellKnownMimeType.fromString(mimes.get(0)));
  }

  @Test
  public void multipleAndMixMimeType() {
    List<String> mimeTypes =
        Lists.newArrayList("aaa/bbb", WellKnownMimeType.APPLICATION_HESSIAN.getString());
    ByteBuf byteBuf = MimeTypeMetadataCodec.encode(ByteBufAllocator.DEFAULT, mimeTypes);
    List<String> decodedMimeTypes = MimeTypeMetadataCodec.decode(byteBuf);
    Assertions.assertThat(decodedMimeTypes).isEqualTo(mimeTypes);
  }
}
