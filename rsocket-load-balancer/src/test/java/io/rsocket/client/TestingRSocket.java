/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.client;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSource;

public class TestingRSocket implements RSocket {

  private final AtomicInteger count;
  private final MonoProcessor<Void> closeSubject = MonoProcessor.create();
  private final BiFunction<Subscriber<? super Payload>, Payload, Boolean> eachPayloadHandler;

  public TestingRSocket(Function<Payload, Payload> responder) {
    this(
        (subscriber, payload) -> {
          subscriber.onNext(responder.apply(payload));
          return true;
        });
  }

  public TestingRSocket(
      BiFunction<Subscriber<? super Payload>, Payload, Boolean> eachPayloadHandler) {
    this.eachPayloadHandler = eachPayloadHandler;
    this.count = new AtomicInteger(0);
  }

  public int countMessageReceived() {
    return count.get();
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return Mono.empty();
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return MonoSource.wrap(
        subscriber ->
            subscriber.onSubscribe(
                new Subscription() {
                  boolean cancelled;

                  @Override
                  public void request(long n) {
                    if (cancelled) {
                      return;
                    }
                    try {
                      count.incrementAndGet();
                      if (eachPayloadHandler.apply(subscriber, payload)) {
                        subscriber.onComplete();
                      }
                    } catch (Throwable t) {
                      subscriber.onError(t);
                    }
                  }

                  @Override
                  public void cancel() {}
                }));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return requestResponse(payload).flux();
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> inputs) {
    return Flux.from(
        subscriber ->
            inputs.subscribe(
                new Subscriber<Payload>() {
                  @Override
                  public void onSubscribe(Subscription s) {
                    subscriber.onSubscribe(s);
                  }

                  @Override
                  public void onNext(Payload input) {
                    eachPayloadHandler.apply(subscriber, input);
                  }

                  @Override
                  public void onError(Throwable t) {
                    subscriber.onError(t);
                  }

                  @Override
                  public void onComplete() {
                    subscriber.onComplete();
                  }
                }));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return fireAndForget(payload);
  }

  @Override
  public double availability() {
    return 1.0;
  }

  @Override
  public Mono<Void> close() {
    return Mono.defer(
        () -> {
          closeSubject.onComplete();
          return closeSubject;
        });
  }

  @Override
  public Mono<Void> onClose() {
    return closeSubject;
  }
}
