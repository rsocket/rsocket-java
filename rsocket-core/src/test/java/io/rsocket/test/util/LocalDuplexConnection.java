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

package io.rsocket.test.util;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class LocalDuplexConnection implements DuplexConnection {
  private final DirectProcessor<Frame> send;
  private final DirectProcessor<Frame> receive;
  private final MonoProcessor<Void> onClose;
  private final String name;

  public LocalDuplexConnection(
      String name, DirectProcessor<Frame> send, DirectProcessor<Frame> receive) {
    this.name = name;
    this.send = send;
    this.receive = receive;
    onClose = MonoProcessor.create();
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frame) {
    return Flux.from(frame)
        .doOnNext(f -> System.out.println(name + " - " + f.toString()))
        .doOnNext(send::onNext)
        .doOnError(send::onError)
        .then();
  }

  @Override
  public Flux<Frame> receive() {
    return receive.doOnNext(f -> System.out.println(name + " - " + f.toString()));
  }

  @Override
  public void dispose() {
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
}
