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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.util.annotation.Nullable;

/** An RSocket frame that is fragmentable */
public interface FragmentableFrame extends MetadataAndDataFrame {

  /**
   * Generates the fragment for this frame.
   *
   * @param byteBufAllocator the {@link ByteBufAllocator} to use
   * @param metadata the metadata
   * @param data the data
   * @return the fragment for this frame
   * @throws NullPointerException if {@code ByteBufAllocator} is {@code null}
   */
  FragmentableFrame createFragment(
      ByteBufAllocator byteBufAllocator, @Nullable ByteBuf metadata, @Nullable ByteBuf data);

  /**
   * Generates the non-fragment for this frame.
   *
   * @param byteBufAllocator the {@link ByteBufAllocator} to use
   * @param metadata the metadata
   * @param data the data
   * @return the non-fragment for this frame
   * @throws NullPointerException if {@code ByteBufAllocator} is {@code null}
   */
  FragmentableFrame createNonFragment(
      ByteBufAllocator byteBufAllocator, @Nullable ByteBuf metadata, @Nullable ByteBuf data);

  /**
   * Returns whether the Follows flag is set.
   *
   * @return whether the Follows flag is set
   */
  boolean isFollowsFlagSet();
}
