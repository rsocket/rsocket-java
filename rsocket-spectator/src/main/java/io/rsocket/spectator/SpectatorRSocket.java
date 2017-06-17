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

package io.rsocket.spectator;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import java.lang.reflect.Array;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Wraps a {@link RSocket} with counters */
public class SpectatorRSocket implements RSocket {
  private final RSocket delegate;

  private Counter fireAndForgetErrors;
  private Counter fireAndForgetCanceled;
  private Counter fireAndForgetTotal;
  private Timer fireAndForgetTimer;

  private Counter requestResponseErrors;
  private Counter requestResponseCanceled;
  private Counter requestResponseTotal;
  private Timer requestResponseTimer;

  private Counter requestStreamErrors;
  private Counter requestStreamCanceled;
  private Counter requestStreamTotal;

  private Counter requestChannelErrors;
  private Counter requestChannelCanceled;
  private Counter requestChannelTotal;

  private Counter metadataPushErrors;
  private Counter metadataPushCanceled;
  private Counter metadataPushTotal;
  private Timer metadataPushTimer;

  public SpectatorRSocket(Registry registry, RSocket delegate, String... tags) {
    this.delegate = delegate;

    this.fireAndForgetErrors =
        registry.counter("reactiveSocketStats", concatenate(tags, "fireAndForget", "errors"));
    this.fireAndForgetCanceled =
        registry.counter("reactiveSocketStats", concatenate(tags, "fireAndForget", "canceled"));
    this.fireAndForgetTotal =
        registry.counter("reactiveSocketStats", concatenate(tags, "fireAndForget", "total"));
    this.fireAndForgetTimer =
        registry.timer("reactiveSocketStats", concatenate(tags, "fireAndForget", "timer"));

    this.requestResponseErrors =
        registry.counter("reactiveSocketStats", concatenate(tags, "requestResponse", "errors"));
    this.requestResponseCanceled =
        registry.counter("reactiveSocketStats", concatenate(tags, "requestResponse", "canceled"));
    this.requestResponseTotal =
        registry.counter("reactiveSocketStats", concatenate(tags, "requestResponse", "total"));
    this.requestResponseTimer =
        registry.timer("reactiveSocketStats", concatenate(tags, "requestResponse", "timer"));

    this.requestStreamErrors =
        registry.counter("reactiveSocketStats", concatenate(tags, "requestStream", "errors"));
    this.requestStreamCanceled =
        registry.counter("reactiveSocketStats", concatenate(tags, "requestStream", "canceled"));
    this.requestStreamTotal =
        registry.counter("reactiveSocketStats", concatenate(tags, "requestStream", "total"));

    this.requestChannelErrors =
        registry.counter("reactiveSocketStats", concatenate(tags, "requestChannel", "errors"));
    this.requestChannelCanceled =
        registry.counter("reactiveSocketStats", concatenate(tags, "requestChannel", "canceled"));
    this.requestChannelTotal =
        registry.counter("reactiveSocketStats", concatenate(tags, "requestChannel", "total"));

    this.metadataPushErrors =
        registry.counter("reactiveSocketStats", concatenate(tags, "metadataPush", "errors"));
    this.metadataPushCanceled =
        registry.counter("reactiveSocketStats", concatenate(tags, "metadataPush", "canceled"));
    this.metadataPushTotal =
        registry.counter("reactiveSocketStats", concatenate(tags, "metadataPush", "total"));
    this.metadataPushTimer =
        registry.timer("reactiveSocketStats", concatenate(tags, "metadataPush", "timer"));
  }

  static <T> T[] concatenate(T[] a, T... b) {
    if (a == null || a.length == 0) {
      return b;
    }

    int aLen = a.length;
    int bLen = b.length;

    @SuppressWarnings("unchecked")
    T[] c = (T[]) Array.newInstance(a.getClass().getComponentType(), aLen + bLen);
    System.arraycopy(a, 0, c, 0, aLen);
    System.arraycopy(b, 0, c, aLen, bLen);

    return c;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return Mono.defer(
        () -> {
          long start = System.nanoTime();
          return delegate
              .fireAndForget(payload)
              .doFinally(
                  signalType -> {
                    fireAndForgetTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    fireAndForgetTotal.increment();

                    switch (signalType) {
                      case CANCEL:
                        fireAndForgetCanceled.increment();
                        break;
                      case ON_ERROR:
                        fireAndForgetErrors.increment();
                    }
                  });
        });
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return Mono.defer(
        () -> {
          long start = System.nanoTime();
          return delegate
              .requestResponse(payload)
              .doFinally(
                  signalType -> {
                    requestResponseTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    requestResponseTotal.increment();

                    switch (signalType) {
                      case CANCEL:
                        requestResponseCanceled.increment();
                        break;
                      case ON_ERROR:
                        requestResponseErrors.increment();
                    }
                  });
        });
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return Flux.defer(
        () ->
            delegate
                .requestStream(payload)
                .doFinally(
                    signalType -> {
                      requestStreamTotal.increment();

                      switch (signalType) {
                        case CANCEL:
                          requestStreamCanceled.increment();
                          break;
                        case ON_ERROR:
                          requestStreamErrors.increment();
                      }
                    }));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return Flux.defer(
        () ->
            delegate
                .requestChannel(payloads)
                .doFinally(
                    signalType -> {
                      requestChannelTotal.increment();

                      switch (signalType) {
                        case CANCEL:
                          requestChannelCanceled.increment();
                          break;
                        case ON_ERROR:
                          requestChannelErrors.increment();
                      }
                    }));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return Mono.defer(
        () -> {
          long start = System.nanoTime();
          return delegate
              .metadataPush(payload)
              .doFinally(
                  signalType -> {
                    metadataPushTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    metadataPushTotal.increment();

                    switch (signalType) {
                      case CANCEL:
                        metadataPushCanceled.increment();
                        break;
                      case ON_ERROR:
                        metadataPushErrors.increment();
                    }
                  });
        });
  }

  @Override
  public Mono<Void> close() {
    return delegate.close();
  }

  @Override
  public Mono<Void> onClose() {
    return delegate.onClose();
  }

  @Override
  public double availability() {
    return delegate.availability();
  }
}
