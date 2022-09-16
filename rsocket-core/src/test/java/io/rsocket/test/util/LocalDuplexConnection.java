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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import java.net.SocketAddress;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;

public class LocalDuplexConnection implements DuplexConnection {
  private final ByteBufAllocator allocator;
  private final Sinks.Many<ByteBuf> send;
  private final Sinks.Many<ByteBuf> receive;
  private final Sinks.Empty<Void> onClose;
  private final String name;

  public LocalDuplexConnection(
      String name,
      ByteBufAllocator allocator,
      Sinks.Many<ByteBuf> send,
      Sinks.Many<ByteBuf> receive) {
    this.name = name;
    this.allocator = allocator;
    this.send = send;
    this.receive = receive;
    this.onClose = Sinks.empty();
  }

  @Override
  public void sendFrame(int streamId, ByteBuf frame) {
    System.out.println(name + " - " + frame.toString());
    send.tryEmitNext(frame);
  }

  @Override
  public void sendErrorAndClose(RSocketErrorException e) {
    final ByteBuf errorFrame = ErrorFrameCodec.encode(allocator, 0, e);
    System.out.println(name + " - " + errorFrame.toString());
    send.tryEmitNext(errorFrame);
    onClose.tryEmitEmpty();
  }

  @Override
  public Flux<ByteBuf> receive() {
    return receive
        .asFlux()
        .doOnNext(f -> System.out.println(name + " - " + f.toString()))
        .transform(
            Operators.<ByteBuf, ByteBuf>lift(
                (__, actual) ->
                    new CoreSubscriber<ByteBuf>() {

                      @Override
                      public void onSubscribe(Subscription s) {
                        actual.onSubscribe(s);
                      }

                      @Override
                      public void onNext(ByteBuf byteBuf) {
                        actual.onNext(byteBuf);
                        byteBuf.release();
                      }

                      @Override
                      public void onError(Throwable t) {
                        actual.onError(t);
                      }

                      @Override
                      public void onComplete() {
                        actual.onComplete();
                      }
                    }));
  }

  @Override
  public ByteBufAllocator alloc() {
    return allocator;
  }

  @Override
  public SocketAddress localAddress() {
    return new TestLocalSocketAddress(name);
  }

  @Override
  public SocketAddress remoteAddress() {
    return new TestLocalSocketAddress(name);
  }

  @Override
  public void dispose() {
    onClose.tryEmitEmpty();
  }

  @Override
  @SuppressWarnings("ConstantConditions")
  public boolean isDisposed() {
    return onClose.scan(Scannable.Attr.TERMINATED) || onClose.scan(Scannable.Attr.CANCELLED);
  }

  @Override
  public Mono<Void> onClose() {
    return onClose.asMono();
  }
}
