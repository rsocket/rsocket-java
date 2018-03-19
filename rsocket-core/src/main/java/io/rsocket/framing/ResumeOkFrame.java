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

import static io.rsocket.framing.FrameType.RESUME_OK;
import static io.rsocket.util.RecyclerFactory.createRecycler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Objects;

/**
 * An RSocket {@code RESUME_OK} frame.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#resume_ok-frame-0x0e">Resume
 *     OK Frame</a>
 */
public final class ResumeOkFrame extends AbstractRecyclableFrame<ResumeOkFrame> {

  private static final int OFFSET_LAST_RECEIVED_CLIENT_POSITION = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final Recycler<ResumeOkFrame> RECYCLER = createRecycler(ResumeOkFrame::new);

  private ResumeOkFrame(Handle<ResumeOkFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code RESUME_OK} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code RESUME_OK} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static ResumeOkFrame createResumeOkFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code RESUME_OK} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param lastReceivedClientPosition the last received server position
   * @return the {@code RESUME_OK} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static ResumeOkFrame createResumeOkFrame(
      ByteBufAllocator byteBufAllocator, long lastReceivedClientPosition) {

    ByteBuf byteBuf =
        createFrameTypeAndFlags(byteBufAllocator, RESUME_OK).writeLong(lastReceivedClientPosition);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  /**
   * Returns the last received client position.
   *
   * @return the last received client position
   */
  public long getLastReceivedClientPosition() {
    return getByteBuf().getLong(OFFSET_LAST_RECEIVED_CLIENT_POSITION);
  }

  @Override
  public String toString() {
    return "ResumeOkFrame{" + "lastReceivedClientPosition=" + getLastReceivedClientPosition() + '}';
  }
}
