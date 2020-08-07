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

package io.rsocket.test;

import static java.util.concurrent.locks.LockSupport.parkNanos;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TestRSocket implements RSocket {
  private final String data;
  private final String metadata;

  private final AtomicLong observedInteractions = new AtomicLong();
  private final AtomicLong activeInteractions = new AtomicLong();

  public TestRSocket(String data, String metadata) {
    this.data = data;
    this.metadata = metadata;
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    activeInteractions.getAndIncrement();
    payload.release();
    observedInteractions.getAndIncrement();
    return Mono.just(ByteBufPayload.create(data, metadata))
        .doFinally(__ -> activeInteractions.getAndDecrement());
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    activeInteractions.getAndIncrement();
    payload.release();
    observedInteractions.getAndIncrement();
    return Flux.range(1, 10_000)
        .map(l -> ByteBufPayload.create(data, metadata))
        .doFinally(__ -> activeInteractions.getAndDecrement());
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    activeInteractions.getAndIncrement();
    payload.release();
    observedInteractions.getAndIncrement();
    return Mono.<Void>empty().doFinally(__ -> activeInteractions.getAndDecrement());
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    activeInteractions.getAndIncrement();
    payload.release();
    observedInteractions.getAndIncrement();
    return Mono.<Void>empty().doFinally(__ -> activeInteractions.getAndDecrement());
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    activeInteractions.getAndIncrement();
    observedInteractions.getAndIncrement();
    return Flux.from(payloads).doFinally(__ -> activeInteractions.getAndDecrement());
  }

  public boolean awaitAllInteractionTermination(Duration duration) {
    long end = duration.plusNanos(System.nanoTime()).toNanos();
    long activeNow;
    while ((activeNow = activeInteractions.get()) > 0) {
      if (System.nanoTime() >= end) {
        return false;
      }
      parkNanos(100);
    }

    return activeNow == 0;
  }

  public boolean awaitUntilObserved(int interactions, Duration duration) {
    long end = duration.plusNanos(System.nanoTime()).toNanos();
    long observed;
    while ((observed = observedInteractions.get()) < interactions) {
      if (System.nanoTime() >= end) {
        return false;
      }
      parkNanos(100);
    }

    return observed >= interactions;
  }
}
