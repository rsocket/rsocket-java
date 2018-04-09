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
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import reactor.core.Disposable;

/**
 * An RSocket frame.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#framing">Framing</a>
 */
public interface Frame extends Disposable {

  /** The shift length for the frame type. */
  int FRAME_TYPE_SHIFT = Short.SIZE - FrameType.ENCODED_SIZE;

  /**
   * Exposes the {@code Frame} as a {@link ByteBuf} for consumption.
   *
   * @param consumer the {@link Consumer} to consume the {@code Frame} as a {@link ByteBuf}
   * @throws NullPointerException if {@code consumer} is {@code null}
   */
  default void consumeFrame(Consumer<ByteBuf> consumer) {
    Objects.requireNonNull(consumer, "consumer must not be null");

    consumer.accept(getUnsafeFrame());
  }

  /**
   * Returns the {@link FrameType}.
   *
   * @return the {@link FrameType}
   */
  FrameType getFrameType();

  /**
   * Returns the frame directly.
   *
   * <p><b>Note:</b> this frame will be outside of the {@code Frame}'s lifecycle and may be released
   * at any time. It is highly recommended that you {@link ByteBuf#retain()} the frame if you store
   * it.
   *
   * @return the frame directly
   * @see #consumeFrame(Consumer)
   * @see #mapFrame(Function)
   */
  ByteBuf getUnsafeFrame();

  /**
   * Exposes the {@code Frame} as a {@link ByteBuf} for mapping to a different type.
   *
   * @param function the {@link Function} to transform the {@code Frame} as a {@link ByteBuf} to a
   *     different type
   * @param <T> the different type
   * @return the {@code Frame} as a {@link ByteBuf} mapped to a different type
   * @throws NullPointerException if {@code function} is {@code null}
   */
  default <T> T mapFrame(Function<ByteBuf, T> function) {
    Objects.requireNonNull(function, "function must not be null");

    return function.apply(getUnsafeFrame());
  }
}
