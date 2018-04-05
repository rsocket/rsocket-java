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
import static io.rsocket.test.util.ByteBufUtils.getRandomByteBuf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

interface FragmentableFrameTest<T extends FragmentableFrame> extends MetadataAndDataFrameTest<T> {

  @DisplayName("creates fragment")
  @Test
  default void createFragment() {
    ByteBuf metadata = getRandomByteBuf(2);
    ByteBuf data = getRandomByteBuf(2);

    FragmentableFrame frame =
        getFrameWithoutFollowsFlagSet().createFragment(DEFAULT, metadata, data);

    assertThat(frame.isFollowsFlagSet()).isTrue();
    assertThat(frame.mapMetadata(Function.identity())).hasValue(metadata);
    assertThat(frame.mapData(Function.identity())).isEqualTo(data);
  }

  @DisplayName("createFragment throws NullPointerException with null ByteBufAllocator")
  @Test
  default void createInitialFragmentNullByteBufAllocator() {
    assertThatNullPointerException()
        .isThrownBy(() -> getFrameWithoutFollowsFlagSet().createFragment(null, null, null))
        .withMessage("byteBufAllocator must not be null");
  }

  @DisplayName("creates non-fragment")
  @Test
  default void createNonFragment() {
    ByteBuf metadata = getRandomByteBuf(2);
    ByteBuf data = getRandomByteBuf(2);

    FragmentableFrame frame =
        getFrameWithoutFollowsFlagSet().createNonFragment(DEFAULT, metadata, data);

    assertThat(frame.isFollowsFlagSet()).isFalse();
    assertThat(frame.mapMetadata(Function.identity())).hasValue(metadata);
    assertThat(frame.mapData(Function.identity())).isEqualTo(data);
  }

  T getFrameWithFollowsFlagSet();

  T getFrameWithoutFollowsFlagSet();

  @DisplayName("tests follows flag not set")
  @Test
  default void isFollowFlagSetFalse() {
    assertThat(getFrameWithoutFollowsFlagSet().isFollowsFlagSet()).isFalse();
  }

  @DisplayName("tests follows flag set")
  @Test
  default void isFollowFlagSetTrue() {
    assertThat(getFrameWithFollowsFlagSet().isFollowsFlagSet()).isTrue();
  }
}
