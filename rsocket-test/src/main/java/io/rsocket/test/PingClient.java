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

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.function.BiFunction;
import org.HdrHistogram.Recorder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class PingClient {

  private final Payload payload;
  private final Mono<RSocket> client;

  public PingClient(Mono<RSocket> client) {
    this.client = client;
    this.payload = ByteBufPayload.create("hello");
  }

  public Recorder startTracker(Duration interval) {
    final Recorder histogram = new Recorder(3600000000000L, 3);
    Flux.interval(interval)
        .doOnNext(
            aLong -> {
              System.out.println("---- PING/ PONG HISTO ----");
              histogram
                  .getIntervalHistogram()
                  .outputPercentileDistribution(System.out, 5, 1000.0, false);
              System.out.println("---- PING/ PONG HISTO ----");
            })
        .subscribe();
    return histogram;
  }

  public Flux<Payload> requestResponsePingPong(int count, final Recorder histogram) {
    return pingPong(RSocket::requestResponse, count, histogram);
  }

  public Flux<Payload> requestStreamPingPong(int count, final Recorder histogram) {
    return pingPong(RSocket::requestStream, count, histogram);
  }

  Flux<Payload> pingPong(
      BiFunction<RSocket, ? super Payload, ? extends Publisher<Payload>> interaction,
      int count,
      final Recorder histogram) {
    return Flux.usingWhen(
            client,
            rsocket ->
                Flux.range(1, count)
                    .flatMap(
                        i -> {
                          long start = System.nanoTime();
                          return Flux.from(interaction.apply(rsocket, payload.retain()))
                              .doOnNext(Payload::release)
                              .doFinally(
                                  signalType -> {
                                    long diff = System.nanoTime() - start;
                                    histogram.recordValue(diff);
                                  });
                        },
                        64),
            rsocket -> {
              rsocket.dispose();
              return rsocket.onClose();
            })
        .doOnError(Throwable::printStackTrace);
  }
}
