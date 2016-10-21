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

package io.reactivesocket.aeron.internal.reactivestreams;

import io.reactivesocket.aeron.MediaDriverHolder;
import io.reactivesocket.aeron.internal.AeronWrapper;
import io.reactivesocket.aeron.internal.DefaultAeronWrapper;
import io.reactivesocket.aeron.internal.SingleThreadedEventLoop;
import io.reactivesocket.reactivestreams.extensions.Px;
import org.agrona.DirectBuffer;

/**
 *
 */
public class AeronChannelPongServer {
    public static void main(String... args) {
        MediaDriverHolder.getInstance();
        AeronWrapper wrapper = new DefaultAeronWrapper();
        AeronSocketAddress managementSubscription = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        SingleThreadedEventLoop eventLoop = new SingleThreadedEventLoop("server");

        AeronChannelServer.AeronChannelConsumer consumer = new AeronChannelServer.AeronChannelConsumer() {
            @Override
            public void accept(AeronChannel aeronChannel) {
                Px<? extends DirectBuffer> receive =
                    aeronChannel
                        .receive();
                        //.doOnNext(b -> System.out.println("server got => " + b.getInt(0)));

                Px
                    .from(aeronChannel.send(ReactiveStreamsRemote.In.from(receive)))
                    .doOnError(throwable -> throwable.printStackTrace())
                    .subscribe();
            }
        };

        AeronChannelServer server = AeronChannelServer.create(consumer, wrapper, managementSubscription, eventLoop);
        AeronChannelServer.AeronChannelStartedServer start = server.start();
        start.awaitShutdown();
    }
}
