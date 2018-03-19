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

import static io.rsocket.framing.FrameType.CANCEL;
import static io.rsocket.util.RecyclerFactory.createRecycler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Objects;

/**
 * An RSocket {@code CANCEL} frame.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#cancel-frame-0x09">Cancel
 *     Frame</a>
 */
public final class CancelFrame extends AbstractRecyclableFrame<CancelFrame> {

  private static final Recycler<CancelFrame> RECYCLER = createRecycler(CancelFrame::new);

  private CancelFrame(Handle<CancelFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code CANCEL} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code CANCEL} frame
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static CancelFrame createCancelFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code CANCEL} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @return the {@code CANCEL} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static CancelFrame createCancelFrame(ByteBufAllocator byteBufAllocator) {
    ByteBuf byteBuf = createFrameTypeAndFlags(byteBufAllocator, CANCEL);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  @Override
  public String toString() {
    return "CancelFrame{} ";
  }
}
