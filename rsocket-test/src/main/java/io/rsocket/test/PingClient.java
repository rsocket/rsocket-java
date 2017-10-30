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

package io.rsocket.test;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import org.HdrHistogram.Recorder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class PingClient {

  private final Payload payload;
  private final Mono<RSocket> client;

  public PingClient(Mono<RSocket> client) {
    this.client = client;
    this.payload = new PayloadImpl("hello");
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

  public Flux<Payload> startPingPong(int count, final Recorder histogram) {
    return client
        .flatMapMany(
            rsocket ->
                Flux.range(1, count)
                    .flatMap(
                        i -> {
                          long start = System.nanoTime();
                          return rsocket
                              .requestResponse(payload)
                              .doFinally(
                                  signalType -> {
                                    long diff = System.nanoTime() - start;
                                    histogram.recordValue(diff);
                                  });
                        },
                        64))
        .doOnError(Throwable::printStackTrace);
  }
}
