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

import io.rsocket.aeron.MediaDriverHolder;
import io.rsocket.aeron.internal.AeronWrapper;
import io.rsocket.aeron.internal.DefaultAeronWrapper;
import io.rsocket.aeron.internal.SingleThreadedEventLoop;
import org.agrona.DirectBuffer;
import reactor.core.publisher.Flux;

/** */
public class AeronChannelPongServer {
  public static void main(String... args) {
    MediaDriverHolder.getInstance();
    AeronWrapper wrapper = new DefaultAeronWrapper();
    AeronSocketAddress managementSubscription =
        AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
    SingleThreadedEventLoop eventLoop = new SingleThreadedEventLoop("server");

    AeronChannelServer.AeronChannelConsumer consumer =
        aeronChannel -> {
          Flux<? extends DirectBuffer> receive = aeronChannel.receive();
          // .doOnNext(b -> System.out.println("server got => " + b.getInt(0)));

          aeronChannel.send(receive).doOnError(Throwable::printStackTrace).subscribe();
        };

    AeronChannelServer server =
        AeronChannelServer.create(consumer, wrapper, managementSubscription, eventLoop);
    AeronChannelServer.AeronChannelStartedServer start = server.start();
    start.awaitShutdown();
  }
}
