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

package io.rsocket.perfutil;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * An implementation of {@link DuplexConnection} that provides functionality to modify the behavior
 * dynamically.
 */
public class TestDuplexConnection implements DuplexConnection {

  private final DirectProcessor<Frame> send;
  private final DirectProcessor<Frame> receive;

  public TestDuplexConnection(DirectProcessor<Frame> send, DirectProcessor<Frame> receive) {
    this.send = send;
    this.receive = receive;
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frame) {
    return Flux.from(frame)
        .doOnNext(
            f -> {
              try {
                send.onNext(f);
              } finally {
                f.release();
              }
            })
        .then();
  }

  @Override
  public Mono<Void> sendOne(Frame frame) {
    send.onNext(frame);
    return Mono.empty();
  }

  @Override
  public Flux<Frame> receive() {
    return receive;
  }

  @Override
  public double availability() {
    return 1.0;
  }

  @Override
  public void dispose() {}

  @Override
  public Mono<Void> onClose() {
    return Mono.empty();
  }
}
