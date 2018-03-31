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

package io.rsocket.fragmentation;

import static io.rsocket.util.DisposableUtil.disposeQuietly;
import static io.rsocket.util.RecyclerFactory.createRecycler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.rsocket.framing.FragmentableFrame;
import io.rsocket.framing.Frame;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import reactor.core.Disposable;
import reactor.util.annotation.Nullable;

/**
 * The implementation of the RSocket reassembly behavior.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#fragmentation-and-reassembly">Fragmentation
 *     and Reassembly</a>
 */
final class FrameReassembler implements Disposable {

  private static final Recycler<FrameReassembler> RECYCLER = createRecycler(FrameReassembler::new);

  private final Handle<FrameReassembler> handle;

  private ByteBufAllocator byteBufAllocator;

  private ReassemblyState state;

  private FrameReassembler(Handle<FrameReassembler> handle) {
    this.handle = handle;
  }

  @Override
  public void dispose() {
    if (state != null) {
      disposeQuietly(state);
    }

    byteBufAllocator = null;
    state = null;

    handle.recycle(this);
  }

  /**
   * Creates a new instance
   *
   * @param byteBufAllocator the {@link ByteBufAllocator} to use
   * @return the {@code FrameReassembler}
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  static FrameReassembler createFrameReassembler(ByteBufAllocator byteBufAllocator) {
    return RECYCLER.get().setByteBufAllocator(byteBufAllocator);
  }

  /**
   * Reassembles a frame. If the frame is not a candidate for fragmentation, emits the frame. If
   * frame is a candidate for fragmentation, accumulates the content until the final fragment.
   *
   * @param frame the frame to inspect for reassembly
   * @return the reassembled frame if complete, otherwise {@code null}
   * @throws NullPointerException if {@code frame} is {@code null}
   */
  @Nullable
  Frame reassemble(Frame frame) {
    Objects.requireNonNull(frame, "frame must not be null");

    if (!(frame instanceof FragmentableFrame)) {
      return frame;
    }

    FragmentableFrame fragmentableFrame = (FragmentableFrame) frame;

    if (fragmentableFrame.isFollowsFlagSet()) {
      if (state == null) {
        state = new ReassemblyState(fragmentableFrame);
      } else {
        state.accumulate(fragmentableFrame);
      }
    } else if (state != null) {
      state.accumulate(fragmentableFrame);

      Frame reassembledFrame = state.createFrame(byteBufAllocator);
      state.dispose();
      state = null;

      return reassembledFrame;
    } else {
      return fragmentableFrame;
    }

    return null;
  }

  FrameReassembler setByteBufAllocator(ByteBufAllocator byteBufAllocator) {
    this.byteBufAllocator =
        Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");

    return this;
  }

  static final class ReassemblyState implements Disposable {

    private ByteBuf data;

    private List<FragmentableFrame> fragments = new ArrayList<>();

    private ByteBuf metadata;

    ReassemblyState(FragmentableFrame fragment) {
      accumulate(fragment);
    }

    @Override
    public void dispose() {
      fragments.forEach(Disposable::dispose);
    }

    void accumulate(FragmentableFrame fragment) {
      fragments.add(fragment);
      metadata = accumulateMetadata(fragment);
      data = accumulateData(fragment);
    }

    Frame createFrame(ByteBufAllocator byteBufAllocator) {
      FragmentableFrame root = fragments.get(0);
      return root.createNonFragment(byteBufAllocator, metadata, data);
    }

    private ByteBuf accumulateData(FragmentableFrame fragment) {
      ByteBuf data = fragment.getUnsafeData();
      return this.data == null ? data.retain() : Unpooled.wrappedBuffer(this.data, data.retain());
    }

    private @Nullable ByteBuf accumulateMetadata(FragmentableFrame fragment) {
      ByteBuf metadata = fragment.getUnsafeMetadata();

      if (metadata == null) {
        return this.metadata;
      }

      return this.metadata == null
          ? metadata.retain()
          : Unpooled.wrappedBuffer(this.metadata, metadata.retain());
    }
  }
}
