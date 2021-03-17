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
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

public class MimeMetadataCodecTest {

    @Test
    public void customMime() {
        String customMime = "aaa/bb";
        ByteBuf byteBuf = MimeMetadataCodec.encodeCustomMime(ByteBufAllocator.DEFAULT, customMime);
        List<String> mimes = MimeMetadataCodec.decode(byteBuf);
        Assert.assertTrue(mimes.size() == 1);
        Assert.assertEquals(customMime, mimes.get(0));
    }

    @Test
    public void wellKnowMime() {
        WellKnownMimeType wellKnownMimeType = WellKnownMimeType.APPLICATION_HESSIAN;
        ByteBuf byteBuf = MimeMetadataCodec.encodeWellKnowMime(ByteBufAllocator.DEFAULT, wellKnownMimeType);
        List<String> mimes = MimeMetadataCodec.decode(byteBuf);
        Assert.assertTrue(mimes.size() == 1);
        Assert.assertEquals(wellKnownMimeType, WellKnownMimeType.fromString(mimes.get(0)));
    }

    @Test
    public void multipleAndMixTypeMime() {
        List<String> mimes = Lists.newArrayList("aaa/bbb", WellKnownMimeType.APPLICATION_HESSIAN.getString());
        ByteBuf byteBuf = MimeMetadataCodec.encode(ByteBufAllocator.DEFAULT, mimes);
        List<String> decodedMimes = MimeMetadataCodec.decode(byteBuf);
        Assert.assertTrue(mimes.size() == 2);
        Assert.assertTrue(mimes.containsAll(decodedMimes));
        Assert.assertTrue(decodedMimes.containsAll(mimes));
    }

}
