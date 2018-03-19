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
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An abstract implementation of {@link DataFrame} that enables recycling for performance.
 *
 * @param <SELF> the implementing type
 * @see io.netty.util.Recycler
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#metadata-optional-header">Frame
 *     Data</a>
 */
abstract class AbstractRecyclableDataFrame<SELF extends AbstractRecyclableDataFrame<SELF>>
    extends AbstractRecyclableFrame<SELF> implements DataFrame {

  AbstractRecyclableDataFrame(Handle<SELF> handle) {
    super(handle);
  }

  /**
   * Appends data to the {@link ByteBuf}.
   *
   * @param byteBuf the {@link ByteBuf} to append to
   * @param data the data to append
   * @return the {@link ByteBuf} with data appended to it
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  static ByteBuf appendData(ByteBuf byteBuf, @Nullable ByteBuf data) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    if (data == null) {
      return byteBuf;
    }

    return Unpooled.wrappedBuffer(byteBuf, data.retain());
  }

  /**
   * Returns the data.
   *
   * @param dataOffset the offset that the data starts at, relative to start of the {@link ByteBuf}
   * @return the data
   */
  final ByteBuf getData(int dataOffset) {
    ByteBuf byteBuf = getByteBuf();
    return byteBuf.slice(dataOffset, byteBuf.readableBytes() - dataOffset).asReadOnly();
  }
}
