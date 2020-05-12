/*
 * Copyright 2015-2020 the original author or authors.
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
import io.netty.buffer.ByteBufUtil;
import io.rsocket.DuplexConnection;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameLengthCodec;
import io.rsocket.frame.FrameType;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

/**
 * A {@link DuplexConnection} implementation that fragments and reassembles {@link ByteBuf}s.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#fragmentation-and-reassembly">Fragmentation
 *     and Reassembly</a>
 */
public final class FragmentationDuplexConnection extends ReassemblyDuplexConnection
    implements DuplexConnection {
  public static final int MIN_MTU_SIZE = 64;
  private static final Logger logger = LoggerFactory.getLogger(FragmentationDuplexConnection.class);
  private final DuplexConnection delegate;
  private final int mtu;
  private final FrameReassembler frameReassembler;
  private final boolean encodeLength;
  private final String type;

  public FragmentationDuplexConnection(
      DuplexConnection delegate, int mtu, boolean encodeAndEncodeLength, String type) {
    super(delegate, encodeAndEncodeLength);

    Objects.requireNonNull(delegate, "delegate must not be null");
    this.encodeLength = encodeAndEncodeLength;
    this.delegate = delegate;
    this.mtu = assertMtu(mtu);
    this.frameReassembler = new FrameReassembler(delegate.alloc());
    this.type = type;

    delegate.onClose().doFinally(s -> frameReassembler.dispose()).subscribe();
  }

  private boolean shouldFragment(FrameType frameType, int readableBytes) {
    return frameType.isFragmentable() && readableBytes > mtu;
  }

  /*TODO this is nullable and not returning empty to workaround javac 11.0.3 compiler issue on ubuntu (at least) */
  @Nullable
  public static <T> Mono<T> checkMtu(int mtu) {
    if (isInsufficientMtu(mtu)) {
      String msg =
          String.format("smallest allowed mtu size is %d bytes, provided: %d", MIN_MTU_SIZE, mtu);
      return Mono.error(new IllegalArgumentException(msg));
    } else {
      return null;
    }
  }

  private static int assertMtu(int mtu) {
    if (isInsufficientMtu(mtu)) {
      String msg =
          String.format("smallest allowed mtu size is %d bytes, provided: %d", MIN_MTU_SIZE, mtu);
      throw new IllegalArgumentException(msg);
    } else {
      return mtu;
    }
  }

  private static boolean isInsufficientMtu(int mtu) {
    return mtu > 0 && mtu < MIN_MTU_SIZE || mtu < 0;
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return Flux.from(frames).concatMap(this::sendOne).then();
  }

  @Override
  public Mono<Void> sendOne(ByteBuf frame) {
    FrameType frameType = FrameHeaderCodec.frameType(frame);
    int readableBytes = frame.readableBytes();
    if (shouldFragment(frameType, readableBytes)) {
      if (logger.isDebugEnabled()) {
        return delegate.send(
            Flux.from(fragmentFrame(alloc(), mtu, frame, frameType, encodeLength))
                .doOnNext(
                    byteBuf -> {
                      ByteBuf f = encodeLength ? FrameLengthCodec.frame(byteBuf) : byteBuf;
                      logger.debug(
                          "{} - stream id {} - frame type {} - \n {}",
                          type,
                          FrameHeaderCodec.streamId(f),
                          FrameHeaderCodec.frameType(f),
                          ByteBufUtil.prettyHexDump(f));
                    }));
      } else {
        return delegate.send(
            Flux.from(fragmentFrame(alloc(), mtu, frame, frameType, encodeLength)));
      }
    } else {
      return delegate.sendOne(encode(frame));
    }
  }

  private ByteBuf encode(ByteBuf frame) {
    if (encodeLength) {
      return FrameLengthCodec.encode(alloc(), frame.readableBytes(), frame);
    } else {
      return frame;
    }
  }
}
