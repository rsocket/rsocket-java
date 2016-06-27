/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.test;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketConnector;
import io.reactivesocket.util.PayloadImpl;
import org.HdrHistogram.Recorder;
import rx.Observable;
import rx.RxReactiveStreams;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

public class PingClient {

    private final ReactiveSocketConnector<SocketAddress> connector;
    private final String request;
    private ReactiveSocket reactiveSocket;

    public PingClient(ReactiveSocketConnector<SocketAddress> connector) {
        this.connector = connector;
        request = "hello";
    }

    public PingClient connect(SocketAddress address) {
        if (null == reactiveSocket) {
            reactiveSocket = RxReactiveStreams.toObservable(connector.connect(address))
                                              .toSingle()
                                              .toBlocking()
                                              .value();
        }
        return this;
    }

    public Recorder startTracker(long interval, TimeUnit timeUnit) {
        final Recorder histogram = new Recorder(3600000000000L, 3);
        Observable.interval(interval, timeUnit)
                  .forEach(aLong -> {
                      System.out.println("---- PING/ PONG HISTO ----");
                      histogram.getIntervalHistogram()
                               .outputPercentileDistribution(System.out, 5, 1000.0, false);
                      System.out.println("---- PING/ PONG HISTO ----");
                  });
        return histogram;
    }

    public Observable<Payload> startPingPong(int count, final Recorder histogram) {
        connect(new InetSocketAddress("localhost", 7878));
        return Observable.range(1, count)
                .flatMap(i -> {
                    long start = System.nanoTime();
                    return RxReactiveStreams.toObservable(reactiveSocket.requestResponse(new PayloadImpl(request)))
                                            .doOnTerminate(() -> {
                                                long diff = System.nanoTime() - start;
                                                histogram.recordValue(diff);
                                            });
                }, 16)
                .doOnError(Throwable::printStackTrace);
    }
}
