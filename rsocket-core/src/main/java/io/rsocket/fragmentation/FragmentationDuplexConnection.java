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

import static io.rsocket.fragmentation.FrameReassembler.createFrameReassembler;
import static io.rsocket.util.AbstractionLeakingFrameUtils.toAbstractionLeakingFrame;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.util.AbstractionLeakingFrameUtils;
import io.rsocket.util.NumberUtils;
import java.util.Objects;
import org.jctools.maps.NonBlockingHashMapLong;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A {@link DuplexConnection} implementation that fragments and reassembles {@link Frame}s.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#fragmentation-and-reassembly">Fragmentation
 *     and Reassembly</a>
 */
public final class FragmentationDuplexConnection implements DuplexConnection {

  private final ByteBufAllocator byteBufAllocator;

  private final DuplexConnection delegate;

  private final FrameFragmenter frameFragmenter;

  private final NonBlockingHashMapLong<FrameReassembler> frameReassemblers =
      new NonBlockingHashMapLong<>();

  /**
   * Creates a new instance.
   *
   * @param delegate the {@link DuplexConnection} to decorate
   * @param maxFragmentSize the maximum fragment size
   * @throws NullPointerException if {@code delegate} is {@code null}
   * @throws IllegalArgumentException if {@code maxFragmentSize} is not {@code positive}
   */
  // TODO: Remove once ByteBufAllocators are shared
  public FragmentationDuplexConnection(DuplexConnection delegate, int maxFragmentSize) {
    this(PooledByteBufAllocator.DEFAULT, delegate, maxFragmentSize);
  }

  /**
   * Creates a new instance.
   *
   * @param byteBufAllocator the {@link ByteBufAllocator} to use
   * @param delegate the {@link DuplexConnection} to decorate
   * @param maxFragmentSize the maximum fragment size. A value of 0 indicates that frames should not
   *     be fragmented.
   * @throws NullPointerException if {@code byteBufAllocator} or {@code delegate} are {@code null}
   * @throws IllegalArgumentException if {@code maxFragmentSize} is not {@code positive}
   */
  public FragmentationDuplexConnection(
      ByteBufAllocator byteBufAllocator, DuplexConnection delegate, int maxFragmentSize) {

    this.byteBufAllocator =
        Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");

    NumberUtils.requireNonNegative(maxFragmentSize, "maxFragmentSize must be positive");

    this.frameFragmenter = new FrameFragmenter(byteBufAllocator, maxFragmentSize);

    delegate
        .onClose()
        .doFinally(signalType -> frameReassemblers.values().forEach(FrameReassembler::dispose))
        .subscribe();
  }

  @Override
  public double availability() {
    return delegate.availability();
  }

  @Override
  public void dispose() {
    delegate.dispose();
  }

  @Override
  public boolean isDisposed() {
    return delegate.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return delegate.onClose();
  }

  @Override
  public Flux<Frame> receive() {
    return delegate
        .receive()
        .map(AbstractionLeakingFrameUtils::fromAbstractionLeakingFrame)
        .concatMap(t2 -> toReassembledFrames(t2.getT1(), t2.getT2()));
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    Objects.requireNonNull(frames, "frames must not be null");

    return delegate.send(
        Flux.from(frames)
            .map(AbstractionLeakingFrameUtils::fromAbstractionLeakingFrame)
            .concatMap(t2 -> toFragmentedFrames(t2.getT1(), t2.getT2())));
  }

  private Flux<Frame> toFragmentedFrames(int streamId, io.rsocket.framing.Frame frame) {
    return this.frameFragmenter
        .fragment(frame)
        .map(fragment -> toAbstractionLeakingFrame(byteBufAllocator, streamId, fragment));
  }

  private Mono<Frame> toReassembledFrames(int streamId, io.rsocket.framing.Frame fragment) {
    FrameReassembler frameReassembler =
        frameReassemblers.computeIfAbsent(
            (long) streamId, i -> createFrameReassembler(byteBufAllocator));

    return Mono.justOrEmpty(frameReassembler.reassemble(fragment))
        .map(frame -> toAbstractionLeakingFrame(byteBufAllocator, streamId, frame));
  }
}
