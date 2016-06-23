/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.transport.tcp;

import io.netty.util.internal.ThreadLocalRandom;
import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer;
import rx.Observable;
import rx.RxReactiveStreams;

import java.nio.ByteBuffer;

public final class Pong {

    public static void main(String... args) throws Exception {
        byte[] response = new byte[1024];
        ThreadLocalRandom.current().nextBytes(response);

        TcpReactiveSocketServer.create(7878)
                               .start((setupPayload, reactiveSocket) -> {
                                   return new RequestHandler.Builder()
                                           .withRequestResponse(payload -> {
                                               Payload responsePayload = new Payload() {
                                                   ByteBuffer data = ByteBuffer.wrap(response);
                                                   ByteBuffer metadata = ByteBuffer.allocate(0);

                                                   @Override
                                                   public ByteBuffer getData() {
                                                       return data;
                                                   }

                                                   @Override
                                                   public ByteBuffer getMetadata() {
                                                       return metadata;
                                                   }
                                               };
                                               return RxReactiveStreams.toPublisher(Observable.just(responsePayload));
                                           })
                                           .build();
                               }).awaitShutdown();
    }
}
