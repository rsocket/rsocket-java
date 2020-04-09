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
import io.rsocket.DuplexConnection;
import io.rsocket.frame.FrameLengthFlyweight;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A {@link DuplexConnection} implementation that reassembles {@link ByteBuf}s.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#fragmentation-and-reassembly">Fragmentation
 *     and Reassembly</a>
 */
public final class ReassemblyDuplexConnection implements DuplexConnection {
  private final DuplexConnection delegate;
  private final FrameReassembler frameReassembler;
  private final boolean decodeLength;

  public ReassemblyDuplexConnection(
      DuplexConnection delegate, ByteBufAllocator allocator, boolean decodeLength) {
    Objects.requireNonNull(delegate, "delegate must not be null");
    Objects.requireNonNull(allocator, "byteBufAllocator must not be null");
    this.decodeLength = decodeLength;
    this.delegate = delegate;
    this.frameReassembler = new FrameReassembler(allocator);

    delegate.onClose().doFinally(s -> frameReassembler.dispose()).subscribe();
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return delegate.send(frames);
  }

  @Override
  public Mono<Void> sendOne(ByteBuf frame) {
    return delegate.sendOne(frame);
  }

  private ByteBuf decode(ByteBuf frame) {
    if (decodeLength) {
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
