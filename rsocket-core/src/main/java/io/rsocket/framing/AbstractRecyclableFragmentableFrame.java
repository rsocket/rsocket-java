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
import io.netty.util.Recycler.Handle;
import java.util.Objects;

/**
 * An abstract implementation of {@link FragmentableFrame} that enables recycling for performance.
 *
 * @param <SELF> the implementing type
 * @see io.netty.util.Recycler
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#metadata-optional-header">Frame
 *     Metadata and Data</a>
 */
abstract class AbstractRecyclableFragmentableFrame<
        SELF extends AbstractRecyclableFragmentableFrame<SELF>>
    extends AbstractRecyclableMetadataAndDataFrame<SELF> implements FragmentableFrame {

  private static final int FLAG_FOLLOWS = 1 << 7;

  AbstractRecyclableFragmentableFrame(Handle<SELF> handle) {
    super(handle);
  }

  @Override
  public final boolean isFollowsFlagSet() {
    return isFlagSet(FLAG_FOLLOWS);
  }

  /**
   * Sets the Follows flag.
   *
   * @param byteBuf the {@link ByteBuf} to set the Follows flag on
   * @return the {@link ByteBuf} with the Follows flag set
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  static ByteBuf setFollowsFlag(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return setFlag(byteBuf, FLAG_FOLLOWS);
  }
}
