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

import static io.rsocket.framing.FrameType.KEEPALIVE;
import static io.rsocket.util.RecyclerFactory.createRecycler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An RSocket {@code KEEPALIVE} frame.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#keepalive-frame-0x03">Keeplive
 *     Frame</a>
 */
public final class KeepaliveFrame extends AbstractRecyclableDataFrame<KeepaliveFrame> {

  private static final int FLAG_RESPOND = 1 << 7;

  private static final int OFFSET_LAST_RECEIVED_POSITION = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final int OFFSET_DATA = OFFSET_LAST_RECEIVED_POSITION + Long.BYTES;

  private static final Recycler<KeepaliveFrame> RECYCLER = createRecycler(KeepaliveFrame::new);

  private KeepaliveFrame(Handle<KeepaliveFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code KEEPALIVE} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code KEEPALIVE} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static KeepaliveFrame createKeepaliveFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code KEEPALIVE} frame.
   *
   * @param byteBufAllocator the {@link ByteBufAllocator} to use
   * @param respond whether to set the Respond flag
   * @param lastReceivedPosition the last received position
   * @param data the frame data
   * @return the {@code KEEPALIVE} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static KeepaliveFrame createKeepaliveFrame(
      ByteBufAllocator byteBufAllocator,
      boolean respond,
      long lastReceivedPosition,
      @Nullable ByteBuf data) {

    ByteBuf byteBuf =
        createFrameTypeAndFlags(byteBufAllocator, KEEPALIVE).writeLong(lastReceivedPosition);

    if (respond) {
      byteBuf = setFlag(byteBuf, FLAG_RESPOND);
    }

    byteBuf = appendData(byteBuf, data);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  /**
   * Returns the last received position.
   *
   * @return the last received position
   */
  public long getLastReceivedPosition() {
    return getByteBuf().getLong(OFFSET_LAST_RECEIVED_POSITION);
  }

  @Override
  public ByteBuf getUnsafeData() {
    return getData(OFFSET_DATA);
  }

  /**
   * Returns whether the respond flag is set.
   *
   * @return whether the respond flag is set
   */
  public boolean isRespondFlagSet() {
    return isFlagSet(FLAG_RESPOND);
  }

  @Override
  public String toString() {
    return "KeepaliveFrame{"
        + "respond="
        + isRespondFlagSet()
        + ", lastReceivedPosition="
        + getLastReceivedPosition()
        + ", data="
        + mapData(ByteBufUtil::hexDump)
        + '}';
  }
}
