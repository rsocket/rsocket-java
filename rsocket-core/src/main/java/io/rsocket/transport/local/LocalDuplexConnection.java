/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.transport.local;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class LocalDuplexConnection implements DuplexConnection {
  private final Flux<Frame> in;
  private final Subscriber<Frame> out;
  private final MonoProcessor<Void> closeNotifier;

  public LocalDuplexConnection(
      Flux<Frame> in, Subscriber<Frame> out, MonoProcessor<Void> closeNotifier) {
    this.in = in;
    this.out = out;
    this.closeNotifier = closeNotifier;
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    return Flux.from(frames).flatMapSequential(this::sendOne).then();
  }

  @Override
  public Mono<Void> sendOne(Frame frame) {
    return Mono.fromRunnable(() -> out.onNext(frame));
  }

  @Override
  public Flux<Frame> receive() {
    return in;
  }

  @Override
  public Mono<Void> close() {
    return Mono.defer(
        () -> {
          out.onComplete();
          closeNotifier.onComplete();
          return closeNotifier;
        });
  }

  @Override
  public Mono<Void> onClose() {
    return closeNotifier;
  }

  @Override
  public double availability() {
    return closeNotifier.isDisposed() ? 0.0 : 1.0;
  }
}
