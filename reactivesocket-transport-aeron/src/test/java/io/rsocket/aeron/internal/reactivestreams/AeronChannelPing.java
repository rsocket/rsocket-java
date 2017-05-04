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

package io.rsocket.aeron.internal.reactivestreams;

import io.rsocket.aeron.internal.AeronWrapper;
import io.rsocket.aeron.internal.DefaultAeronWrapper;
import io.rsocket.aeron.internal.SingleThreadedEventLoop;
import org.HdrHistogram.Recorder;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.core.publisher.Flux;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public final class AeronChannelPing {
    public static void main(String... args) throws Exception {
        int count = 1_000_000_000;
        final Recorder histogram = new Recorder(Long.MAX_VALUE, 3);
        Executors
            .newSingleThreadScheduledExecutor()
            .scheduleAtFixedRate(() -> {
                System.out.println("---- PING/ PONG HISTO ----");
                histogram.getIntervalHistogram()
                    .outputPercentileDistribution(System.out, 5, 1000.0, false);
                System.out.println("---- PING/ PONG HISTO ----");
            }, 1, 1, TimeUnit.SECONDS);

        AeronWrapper wrapper = new DefaultAeronWrapper();
        AeronSocketAddress managementSocketAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        SingleThreadedEventLoop eventLoop = new SingleThreadedEventLoop("client");
        AeronClientChannelConnector connector = AeronClientChannelConnector.create(wrapper, managementSocketAddress, eventLoop);

        AeronSocketAddress receiveAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        AeronSocketAddress sendAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);

        AeronClientChannelConnector.AeronClientConfig
            config =
            AeronClientChannelConnector.AeronClientConfig.create(receiveAddress ,sendAddress, 1, 2, eventLoop);

        AeronChannel channel = connector.apply(config).block();

        AtomicLong lastUpdate = new AtomicLong(System.nanoTime());
        channel
            .receive()
            .doOnNext(b -> {
                synchronized (wrapper) {
                    int anInt = b.getInt(0);
                    if (anInt % 1_000 == 0) {
                        long diff = System.nanoTime() - lastUpdate.get();
                        histogram.recordValue(diff);
                        lastUpdate.set(System.nanoTime());
                    }
                }
            })
            .doOnError(throwable -> throwable.printStackTrace())
            .subscribe();

        byte[] b = new byte[1024];
        Flux.range(0, count)
                .flatMap(i -> {

                    UnsafeBuffer buffer = new UnsafeBuffer(b);
                    buffer.putInt(0, i);
                    return channel.send(buffer);
                }, 8)
                .last(null)
                .block();
    }
}
