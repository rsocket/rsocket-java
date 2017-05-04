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

package io.reactivesocket.test;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.util.PayloadImpl;
import org.HdrHistogram.Recorder;
import reactor.core.publisher.Flux;

import java.time.Duration;

public class PingClient {

    private final Payload payload;
    private final ReactiveSocketClient client;
    private ReactiveSocket reactiveSocket;

    public PingClient(ReactiveSocketClient client) {
        this.client = client;
        this.payload = new PayloadImpl("hello");
    }

    public PingClient connect() {
        if (null == reactiveSocket) {
            reactiveSocket = client.connect().block();
        }
        return this;
    }

    public Recorder startTracker(Duration interval) {
        final Recorder histogram = new Recorder(3600000000000L, 3);
        Flux.interval(interval)
                .doOnNext(aLong -> {
                System.out.println("---- PING/ PONG HISTO ----");
                histogram.getIntervalHistogram()
                    .outputPercentileDistribution(System.out, 5, 1000.0, false);
                System.out.println("---- PING/ PONG HISTO ----");
            })
                .subscribe();
        return histogram;
    }

    public Flux<Payload> startPingPong(int count, final Recorder histogram) {
        connect();
        return Flux.range(1, count)
                .flatMap(i -> {
                    long start = System.nanoTime();
                    return reactiveSocket.requestResponse(payload)
                        .doFinally(signalType -> {
                            long diff = System.nanoTime() - start;
                            histogram.recordValue(diff);
                        });
                }, 16)
                .doOnError(Throwable::printStackTrace);
    }
}
