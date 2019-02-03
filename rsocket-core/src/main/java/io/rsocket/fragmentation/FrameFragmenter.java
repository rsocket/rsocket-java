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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Flux;

/**
 * The implementation of the RSocket fragmentation behavior.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#fragmentation-and-reassembly">Fragmentation
 *     and Reassembly</a>
 */
final class FrameFragmenter {
/*
  private final ByteBufAllocator byteBufAllocator;

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final int maxFragmentSize;

  *//**
   * Creates a new instance
   *
   * @param byteBufAllocator the {@link ByteBufAllocator} to use
   * @param maxFragmentSize the maximum size of each fragment
   *//*
  FrameFragmenter(ByteBufAllocator byteBufAllocator, int maxFragmentSize) {
    this.byteBufAllocator =
        Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    this.maxFragmentSize = maxFragmentSize;
  }

  *//**
   * Returns a {@link Flux} of fragments frames
   *
   * @param frame the {@link ByteBuf} to fragment
   * @return a {@link Flux} of fragment frames
   * @throws NullPointerException if {@code frame} is {@code null}
   *//*
  public Flux<ByteBuf> fragment(ByteBuf frame) {
    Objects.requireNonNull(frame, "frame must not be null");

    if (!shouldFragment(frame)) {
      logger.debug("Not fragmenting {}", frame);
      return Flux.just(frame);
    }

    logger.debug("Fragmenting {}", frame);
    return Flux.generate(
        () -> new FragmentationState((FragmentableFrame) frame),
        this::generate,
        FragmentationState::dispose);
  }

  private FragmentationState generate(FragmentationState state, SynchronousSink<ByteBuf> sink) {
    int fragmentLength = maxFragmentSize;

    ByteBuf metadata;
    if (state.hasReadableMetadata()) {
      metadata = state.readMetadataFragment(fragmentLength);
      fragmentLength -= metadata.readableBytes();
    } else {
      metadata = null;
    }

    if (state.hasReadableMetadata()) {
      ByteBuf fragment = state.createFrame(byteBufAllocator, false, metadata, null);
      logger.debug("Fragment {}", fragment);

      sink.next(fragment);
      return state;
    }

    ByteBuf data;
    data = state.hasReadableData() ? state.readDataFragment(fragmentLength) : null;

    if (state.hasReadableData()) {
      ByteBuf fragment = state.createFrame(byteBufAllocator, false, metadata, data);
      logger.debug("Fragment {}", fragment);

      sink.next(fragment);
      return state;
    }

    ByteBuf fragment = state.createFrame(byteBufAllocator, true, metadata, data);
    logger.debug("Final Fragment {}", fragment);

    sink.next(fragment);
    sink.complete();
    return state;
  }

  private int getFragmentableLength(FragmentableFrame fragmentableFrame) {
    return fragmentableFrame.getMetadataLength().orElse(0) + fragmentableFrame.getDataLength();
  }

  private boolean shouldFragment(ByteBuf frame) {
    if (maxFragmentSize == 0 || !(frame instanceof FragmentableFrame)) {
      return false;
    }

    FragmentableFrame fragmentableFrame = (FragmentableFrame) frame;
    return !fragmentableFrame.isFollowsFlagSet()
        && getFragmentableLength(fragmentableFrame) > maxFragmentSize;
  }

  static final class FragmentationState implements Disposable {

    private final FragmentableFrame frame;

    private int dataIndex = 0;

    private boolean initialFragmentCreated = false;

    private int metadataIndex = 0;

    FragmentationState(FragmentableFrame frame) {
      this.frame = frame;
    }

    @Override
    public void dispose() {
      disposeQuietly(frame);
    }

    ByteBuf createFrame(
        ByteBufAllocator byteBufAllocator,
        boolean complete,
        @Nullable ByteBuf metadata,
        @Nullable ByteBuf data) {

      if (initialFragmentCreated) {
        return createPayloadFrame(byteBufAllocator, !complete, data == null, metadata, data);
      } else {
        initialFragmentCreated = true;
        return frame.createFragment(byteBufAllocator, metadata, data);
      }
    }

    boolean hasReadableData() {
      return frame.getDataLength() - dataIndex > 0;
    }

    boolean hasReadableMetadata() {
      Integer metadataLength = frame.getUnsafeMetadataLength();
      return metadataLength != null && metadataLength - metadataIndex > 0;
    }

    ByteBuf readDataFragment(int length) {
      int safeLength = min(length, frame.getDataLength() - dataIndex);

      ByteBuf fragment = frame.getUnsafeData().slice(dataIndex, safeLength);

      dataIndex += fragment.readableBytes();
      return fragment;
    }

    ByteBuf readMetadataFragment(int length) {
      Integer metadataLength = frame.getUnsafeMetadataLength();
      ByteBuf metadata = frame.getUnsafeMetadata();

      if (metadataLength == null || metadata == null) {
        throw new IllegalStateException("Cannot read metadata fragment with no metadata");
      }

      int safeLength = min(length, metadataLength - metadataIndex);

      ByteBuf fragment = metadata.slice(metadataIndex, safeLength);

      metadataIndex += fragment.readableBytes();
      return fragment;
    }
  }*/
}
