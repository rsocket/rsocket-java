/*
 * Copyright 2015-2021 the original author or authors.
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
package io.rsocket.test.util;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.transport.ServerTransport;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public class TestServerTransport implements ServerTransport<Closeable> {
  private final Sinks.One<TestDuplexConnection> connSink = Sinks.one();
  private TestDuplexConnection connection;
  private final LeaksTrackingByteBufAllocator allocator =
      LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

  int maxFrameLength = FRAME_LENGTH_MASK;

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    connSink
        .asMono()
        .flatMap(duplexConnection -> acceptor.apply(duplexConnection))
        .subscribe(ignored -> {}, err -> disposeConnection(), this::disposeConnection);
    return Mono.just(
        new Closeable() {
          @Override
          public Mono<Void> onClose() {
            return connSink.asMono().then();
          }

          @Override
          public void dispose() {
            connSink.tryEmitEmpty();
          }

          @Override
          @SuppressWarnings("ConstantConditions")
          public boolean isDisposed() {
            return connSink.scan(Scannable.Attr.TERMINATED)
                || connSink.scan(Scannable.Attr.CANCELLED);
          }
        });
  }

  private void disposeConnection() {
    TestDuplexConnection c = connection;
    if (c != null) {
      c.dispose();
    }
  }

  public TestDuplexConnection connect() {
    TestDuplexConnection c = new TestDuplexConnection(allocator);
    connection = c;
    connSink.tryEmitValue(c);
    return c;
  }

  public LeaksTrackingByteBufAllocator alloc() {
    return allocator;
  }

  public TestServerTransport withMaxFrameLength(int maxFrameLength) {
    this.maxFrameLength = maxFrameLength;
    return this;
  }

  @Override
  public int maxFrameLength() {
    return maxFrameLength;
  }
}
