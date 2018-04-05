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
import static io.rsocket.framing.LeaseFrame.createLeaseFrame;
import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

final class LeaseFrameTest implements MetadataFrameTest<LeaseFrame> {

  @Override
  public Function<ByteBuf, LeaseFrame> getCreateFrameFromByteBuf() {
    return LeaseFrame::createLeaseFrame;
  }

  @Override
  public Tuple2<LeaseFrame, ByteBuf> getFrame() {
    ByteBuf byteBuf =
        Unpooled.buffer(12)
            .writeShort(0b00001001_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeInt(0b00000000_00000000_00000000_11001000)
            .writeBytes(getRandomByteBuf(2));

    LeaseFrame frame = createLeaseFrame(byteBuf);

    return Tuples.of(frame, byteBuf);
  }

  @Override
  public LeaseFrame getFrameWithEmptyMetadata() {
    return createLeaseFrame(
        Unpooled.buffer(10)
            .writeShort(0b00001001_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeInt(0b00000000_00000000_00000000_11001000));
  }

  @Override
  public Tuple2<LeaseFrame, ByteBuf> getFrameWithMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    LeaseFrame frame =
        createLeaseFrame(
            Unpooled.buffer(12)
                .writeShort(0b00001001_00000000)
                .writeInt(0b00000000_00000000_00000000_01100100)
                .writeInt(0b00000000_00000000_00000000_11001000)
                .writeBytes(metadata, 0, metadata.readableBytes()));

    return Tuples.of(frame, metadata);
  }

  @Override
  public LeaseFrame getFrameWithoutMetadata() {
    return createLeaseFrame(
        Unpooled.buffer(10)
            .writeShort(0b00001000_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeInt(0b00000000_00000000_00000000_11001000));
  }

  @DisplayName("createLeaseFrame throws IllegalArgumentException with invalid numberOfRequests")
  @Test
  void createLeaseFrameFrameInvalidNumberOfRequests() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> createLeaseFrame(DEFAULT, Duration.ofMillis(1), 0, null))
        .withMessage("numberOfRequests must be positive");
  }

  @DisplayName("createLeaseFrame throws IllegalArgumentException with invalid timeToLive")
  @Test
  void createLeaseFrameInvalidTimeToLive() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> createLeaseFrame(DEFAULT, Duration.ofMillis(0), 1, null))
        .withMessage("timeToLive must be a positive duration");
  }

  @DisplayName("creates LEASE frame with metadata")
  @Test
  void createLeaseFrameMetadata() {
    ByteBuf metadata = getRandomByteBuf(2);

    ByteBuf expected =
        Unpooled.buffer(12)
            .writeShort(0b00001001_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeInt(0b00000000_00000000_00000000_11001000)
            .writeBytes(metadata, 0, metadata.readableBytes());

    assertThat(
            createLeaseFrame(DEFAULT, Duration.ofMillis(100), 200, metadata)
                .mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("creates LEASE frame without metadata")
  @Test
  void createLeaseFrameNoMetadata() {
    ByteBuf expected =
        Unpooled.buffer(10)
            .writeShort(0b00001000_00000000)
            .writeInt(0b00000000_00000000_00000000_01100100)
            .writeInt(0b00000000_00000000_00000000_11001000);

    assertThat(
            createLeaseFrame(DEFAULT, Duration.ofMillis(100), 200, null)
                .mapFrame(Function.identity()))
        .isEqualTo(expected);
  }

  @DisplayName("createLeaseFrame throws NullPointerException with null byteBufAllocator")
  @Test
  void createLeaseFrameNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> createLeaseFrame(null, Duration.ofMillis(1), 1, null))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("createLeaseFrame throws NullPointerException with null timeToLive")
  @Test
  void createLeaseFrameNullTimeToLive() {
    assertThatNullPointerException()
        .isThrownBy(() -> createLeaseFrame(DEFAULT, null, 1, null))
        .withMessage("timeToLive must not be null");
  }

  @DisplayName("returns number of requests")
  @Test
  void getNumberOfRequests() {
    LeaseFrame frame =
        createLeaseFrame(
            Unpooled.buffer(10)
                .writeShort(0b00001000_00000000)
                .writeInt(0b00000000_00000000_00000000_01100100)
                .writeInt(0b00000000_00000000_00000000_11001000));

    assertThat(frame.getNumberOfRequests()).isEqualTo(200);
  }

  @DisplayName("returns time to live")
  @Test
  void getTimeToLive() {
    LeaseFrame frame =
        createLeaseFrame(
            Unpooled.buffer(10)
                .writeShort(0b00001000_00000000)
                .writeInt(0b00000000_00000000_00000000_01100100)
                .writeInt(0b00000000_00000000_00000000_11001000));

    assertThat(frame.getTimeToLive()).isEqualTo(Duration.ofMillis(100));
  }
}
