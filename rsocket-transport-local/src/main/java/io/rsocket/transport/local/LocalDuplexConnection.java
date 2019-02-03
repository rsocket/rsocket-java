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

package io.rsocket.transport.local;

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/** An implementation of {@link DuplexConnection} that connects inside the same JVM. */
final class LocalDuplexConnection implements DuplexConnection {

  private final Flux<Frame> in;

  private final MonoProcessor<Void> onClose;

  private final Subscriber<Frame> out;

  /**
   * Creates a new instance.
   *
   * @param in the inbound {@link Frame}s
   * @param out the outbound {@link Frame}s
   * @param onClose the closing notifier
   * @throws NullPointerException if {@code in}, {@code out}, or {@code onClose} are {@code null}
   */
  LocalDuplexConnection(Flux<Frame> in, Subscriber<Frame> out, MonoProcessor<Void> onClose) {
    this.in = Objects.requireNonNull(in, "in must not be null");
    this.out = Objects.requireNonNull(out, "out must not be null");
    this.onClose = Objects.requireNonNull(onClose, "onClose must not be null");
  }

  @Override
  public void dispose() {
    out.onComplete();
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public Flux<ByteBuf> receive() {
    return in;
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    Objects.requireNonNull(frames, "frames must not be null");

    return Flux.from(frames).doOnNext(out::onNext).then();
  }
}
