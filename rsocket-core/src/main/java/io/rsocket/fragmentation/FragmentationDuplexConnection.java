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

import static io.rsocket.fragmentation.FrameFragmenter.fragmentFrame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.rsocket.DuplexConnection;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameLengthFlyweight;
import io.rsocket.frame.FrameType;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A {@link DuplexConnection} implementation that fragments and reassembles {@link ByteBuf}s.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#fragmentation-and-reassembly">Fragmentation
 *     and Reassembly</a>
 */
public final class FragmentationDuplexConnection implements DuplexConnection {
  private static final int MIN_MTU_SIZE = 64;
  private static final Logger logger = LoggerFactory.getLogger(FragmentationDuplexConnection.class);
  private final DuplexConnection delegate;
  private final int mtu;
  private final ByteBufAllocator allocator;
  private final FrameReassembler frameReassembler;
  private final boolean encodeLength;
  private final String type;

  public FragmentationDuplexConnection(
      DuplexConnection delegate,
      ByteBufAllocator allocator,
      int mtu,
      boolean encodeLength,
      String type) {
    Objects.requireNonNull(delegate, "delegate must not be null");
    Objects.requireNonNull(allocator, "byteBufAllocator must not be null");
    if (mtu < MIN_MTU_SIZE) {
      throw new IllegalArgumentException("smallest allowed mtu size is " + MIN_MTU_SIZE + " bytes");
    }
    this.encodeLength = encodeLength;
    this.allocator = allocator;
    this.delegate = delegate;
    this.mtu = mtu;
    this.frameReassembler = new FrameReassembler(allocator);
    this.type = type;

    delegate.onClose().doFinally(s -> frameReassembler.dispose()).subscribe();
  }

  private boolean shouldFragment(FrameType frameType, int readableBytes) {
    return frameType.isFragmentable() && readableBytes > mtu;
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return Flux.from(frames).concatMap(this::sendOne).then();
  }

  @Override
  public Mono<Void> sendOne(ByteBuf frame) {
    FrameType frameType = FrameHeaderFlyweight.frameType(frame);
    int readableBytes = frame.readableBytes();
    if (shouldFragment(frameType, readableBytes)) {
      if (logger.isDebugEnabled()) {
        return delegate.send(
            Flux.from(fragmentFrame(allocator, mtu, frame, frameType, encodeLength))
                .doOnNext(
                    byteBuf -> {
                      ByteBuf frame1;
                      if (encodeLength) {
                        frame1 = FrameLengthFlyweight.frame(byteBuf);
                      } else {
                        frame1 = byteBuf;
                      }

                      logger.debug(
                          "{} - stream id {} - frame type {} - \n {}",
                          type,
                          FrameHeaderFlyweight.streamId(frame1),
                          FrameHeaderFlyweight.frameType(frame1),
                          ByteBufUtil.prettyHexDump(frame1));
                    }));
      } else {
        return delegate.send(
            Flux.from(fragmentFrame(allocator, mtu, frame, frameType, encodeLength)));
      }
    } else {
      return delegate.sendOne(encode(frame));
    }
  }

  private ByteBuf encode(ByteBuf frame) {
    if (encodeLength) {
      return FrameLengthFlyweight.encode(allocator, frame.readableBytes(), frame).retain();
    } else {
      return frame;
    }
  }

  private ByteBuf decode(ByteBuf frame) {
    if (encodeLength) {
      return FrameLengthFlyweight.frame(frame).retain();
    } else {
      return frame;
    }
  }

  @Override
  public Flux<ByteBuf> receive() {
    return delegate
        .receive()
        .handle(
            (byteBuf, sink) -> {
              ByteBuf decode = decode(byteBuf);
              frameReassembler.reassembleFrame(decode, sink);
            });
  }

  @Override
  public Mono<Void> onClose() {
    return delegate.onClose();
  }

  @Override
  public void dispose() {
    delegate.dispose();
  }
}
