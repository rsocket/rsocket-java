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

import static io.rsocket.framing.FrameType.REQUEST_N;
import static io.rsocket.util.RecyclerFactory.createRecycler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.rsocket.util.NumberUtils;
import java.util.Objects;

/**
 * An RSocket {@code REQUEST_N} frame.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-request-n">RequestN
 *     Frame</a>
 */
public final class RequestNFrame extends AbstractRecyclableFrame<RequestNFrame> {

  private static final int OFFSET_REQUEST_N = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final Recycler<RequestNFrame> RECYCLER = createRecycler(RequestNFrame::new);

  private RequestNFrame(Handle<RequestNFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code REQUEST_N} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code REQUEST_N} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static RequestNFrame createRequestNFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code REQUEST_N} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param requestN the size of the request. Must be positive.
   * @return the {@code REQUEST_N} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static RequestNFrame createRequestNFrame(ByteBufAllocator byteBufAllocator, int requestN) {
    NumberUtils.requirePositive(requestN, "requestN must be positive");

    ByteBuf byteBuf = createFrameTypeAndFlags(byteBufAllocator, REQUEST_N).writeInt(requestN);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  @Override
  public String toString() {
    return "RequestNFrame{" + "requestN=" + getRequestN() + '}';
  }

  /**
   * Returns the size of the request.
   *
   * @return the size of the request
   */
  int getRequestN() {
    return getByteBuf().getInt(OFFSET_REQUEST_N);
  }
}
