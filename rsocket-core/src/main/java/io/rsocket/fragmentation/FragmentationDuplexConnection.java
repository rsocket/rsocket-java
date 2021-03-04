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
public final class FragmentationDuplexConnection extends ReassemblyDuplexConnection
    implements DuplexConnection {

  public static final int MIN_MTU_SIZE = 64;

  private static final Logger logger = LoggerFactory.getLogger(FragmentationDuplexConnection.class);

  final DuplexConnection delegate;
  final int mtu;
  final String type;

  /**
   * Class constructor.
   *
   * @param delegate the underlying connection
   * @param mtu the fragment size, greater than {@link #MIN_MTU_SIZE}
   * @param maxInboundPayloadSize the maximum payload size, which can be reassembled from multiple
   *     fragments
   * @param type a label to use for logging purposes
   */
  public FragmentationDuplexConnection(
      DuplexConnection delegate, int mtu, int maxInboundPayloadSize, String type) {
    super(delegate, maxInboundPayloadSize);

    Objects.requireNonNull(delegate, "delegate must not be null");
    this.delegate = delegate;
    this.mtu = assertMtu(mtu);
    this.type = type;
  }

  private boolean shouldFragment(FrameType frameType, int readableBytes) {
    return frameType.isFragmentable() && readableBytes > mtu;
  }

  public static int assertMtu(int mtu) {
    if (mtu > 0 && mtu < MIN_MTU_SIZE || mtu < 0) {
      String msg =
          String.format(
              "The smallest allowed mtu size is %d bytes, provided: %d", MIN_MTU_SIZE, mtu);
      throw new IllegalArgumentException(msg);
    } else {
      return mtu;
    }
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return delegate.send(
        Flux.from(frames)
            .concatMap(
                frame -> {
                  FrameType frameType = FrameHeaderCodec.frameType(frame);
                  int readableBytes = frame.readableBytes();
                  if (!shouldFragment(frameType, readableBytes)) {
                    return Flux.just(frame);
                  }

                  return logFragments(Flux.from(fragmentFrame(alloc(), mtu, frame, frameType)));
                }));
  }

  @Override
  public Mono<Void> sendOne(ByteBuf frame) {
    FrameType frameType = FrameHeaderCodec.frameType(frame);
    int readableBytes = frame.readableBytes();
    if (!shouldFragment(frameType, readableBytes)) {
      return delegate.sendOne(frame);
    }
    Flux<ByteBuf> fragments = Flux.from(fragmentFrame(alloc(), mtu, frame, frameType));
    fragments = logFragments(fragments);
    return delegate.send(fragments);
  }

  protected Flux<ByteBuf> logFragments(Flux<ByteBuf> fragments) {
    if (logger.isDebugEnabled()) {
      fragments =
          fragments.doOnNext(
              byteBuf -> {
                logger.debug(
                    "{} - stream id {} - frame type {} - \n {}",
                    type,
                    FrameHeaderCodec.streamId(byteBuf),
                    FrameHeaderCodec.frameType(byteBuf),
                    ByteBufUtil.prettyHexDump(byteBuf));
              });
    }
    return fragments;
  }
}
