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

package io.rsocket.framing;

import static io.netty.buffer.UnpooledByteBufAllocator.DEFAULT;
import static io.rsocket.framing.MetadataPushFrame.createMetadataPushFrame;
import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

final class MetadataPushFrameTest implements MetadataFrameTest<MetadataPushFrame> {

  @Override
  public Function<ByteBuf, MetadataPushFrame> getCreateFrameFromByteBuf() {
    return MetadataPushFrame::createMetadataPushFrame;
  }

  @Override
  public Tuple2<MetadataPushFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(4).writeShort(0b00110001_00000000).writeBytes(getRandomByteBuf(2));

    MetadataPushFrame frame = createMetadataPushFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @Override
  public MetadataPushFrame getFrameWithEmptyMetadata() {
    return createMetadataPushFrame(Unpooled.buffer(2).writeShort(0b00110001_00000000));
  }

  @Override
  public Tuple2<MetadataPushFrame, ByteBuf> getFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    MetadataPushFrame frame =
        createMetadataPushFrame(
            Unpooled.buffer(4)
                .writeShort(0b00110001_00000000)
                .writeBytes(metadata, 0, metadata.readableBytes()));

    return Tuples.of(frame, metadata);
  }

  @Override
  public MetadataPushFrame getFrameWithoutMetadata() {
    return createMetadataPushFrame(Unpooled.buffer(2).writeShort(0b00110000_00000000));
  }

  @DisplayName("creates METADATA_PUSH frame with ByteBufAllocator")
  @Test
  void createMetadataPushFrameByteBufAllocator() {
    ByteBuf metadata = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(4)
            .writeShort(0b00110001_00000000)
            .writeBytes(metadata, 0, metadata.readableBytes());

    assertThat(createMetadataPushFrame(DEFAULT, metadata).mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("createMetadataPushFrame throws NullPointerException with null metadata")
  @Test
  void createMetadataPushFrameNullMetadata() {
    assertThatNullPointerException()
        .isThrownBy(() -> createMetadataPushFrame(DEFAULT, (ByteBuf) null))
        .withMessage("metadata must not be null");
  }
}
