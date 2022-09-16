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

package io.rsocket.transport.local;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.internal.UnboundedProcessor;
import java.net.SocketAddress;
import java.util.Objects;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

/** An implementation of {@link DuplexConnection} that connects inside the same JVM. */
final class LocalDuplexConnection implements DuplexConnection {

  private final LocalSocketAddress address;
  private final ByteBufAllocator allocator;
  private final Flux<ByteBuf> in;

  private final Mono<Void> onClose;

  private final UnboundedProcessor out;

  /**
   * Creates a new instance.
   *
   * @param name the name assigned to this local connection
   * @param in the inbound {@link ByteBuf}s
   * @param out the outbound {@link ByteBuf}s
   * @param onClose the closing notifier
   * @throws NullPointerException if {@code in}, {@code out}, or {@code onClose} are {@code null}
   */
  LocalDuplexConnection(
      String name,
      ByteBufAllocator allocator,
      Flux<ByteBuf> in,
      UnboundedProcessor out,
      Mono<Void> onClose) {
    this.address = new LocalSocketAddress(name);
    this.allocator = Objects.requireNonNull(allocator, "allocator must not be null");
    this.in = Objects.requireNonNull(in, "in must not be null");
    this.out = Objects.requireNonNull(out, "out must not be null");
    this.onClose = Objects.requireNonNull(onClose, "onClose must not be null");
  }

  @Override
  public void dispose() {
    out.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return out.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public Flux<ByteBuf> receive() {
    return in.transform(
        Operators.<ByteBuf, ByteBuf>lift(
            (__, actual) -> new ByteBufReleaserOperator(actual, this)));
  }

  @Override
  public void sendFrame(int streamId, ByteBuf frame) {
    if (streamId == 0) {
      out.onNextPrioritized(frame);
    } else {
      out.onNext(frame);
    }
  }

  @Override
  public void sendErrorAndClose(RSocketErrorException e) {
    final ByteBuf errorFrame = ErrorFrameCodec.encode(allocator, 0, e);
    out.onNext(errorFrame);
    dispose();
  }

  @Override
  public ByteBufAllocator alloc() {
    return allocator;
  }

  @Override
  public SocketAddress localAddress() {
    return address;
  }

  @Override
  public SocketAddress remoteAddress() {
    return address;
  }

  static class ByteBufReleaserOperator
      implements CoreSubscriber<ByteBuf>, Subscription, Fuseable.QueueSubscription<ByteBuf> {

    final CoreSubscriber<? super ByteBuf> actual;
    final LocalDuplexConnection parent;

    Subscription s;

    public ByteBufReleaserOperator(
        CoreSubscriber<? super ByteBuf> actual, LocalDuplexConnection parent) {
      this.actual = actual;
      this.parent = parent;
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (Operators.validate(this.s, s)) {
        this.s = s;
        actual.onSubscribe(this);
      }
    }

    @Override
    public void onNext(ByteBuf buf) {
      try {
        actual.onNext(buf);
      } finally {
        buf.release();
      }
    }

    @Override
    public void onError(Throwable t) {
      parent.out.onError(t);
      actual.onError(t);
    }

    @Override
    public void onComplete() {
      parent.out.onComplete();
      actual.onComplete();
    }

    @Override
    public void request(long n) {
      s.request(n);
    }

    @Override
    public void cancel() {
      s.cancel();
      parent.out.onComplete();
    }

    @Override
    public int requestFusion(int requestedMode) {
      return Fuseable.NONE;
    }

    @Override
    public ByteBuf poll() {
      throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public int size() {
      throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public boolean isEmpty() {
      throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }
  }
}
