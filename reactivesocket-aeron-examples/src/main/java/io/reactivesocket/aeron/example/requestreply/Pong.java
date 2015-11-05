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
package io.reactivesocket.aeron.example.requestreply;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.aeron.server.ReactiveSocketAeronServer;
import io.reactivesocket.exceptions.SetupException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by rroeser on 8/16/15.
 */
public class Pong {

   public static void main(String... args) {

        String host = System.getProperty("host", "localhost");

        System.out.println("Setting host to => " + host);

        byte[] response = new byte[1024];
        //byte[] response = new byte[1024];
        Random r = new Random();
        r.nextBytes(response);

        System.out.println("Sending data of size => " + response.length);

        ReactiveSocketAeronServer server = ReactiveSocketAeronServer.create(host, 39790, new ConnectionSetupHandler() {
            @Override
            public RequestHandler apply(ConnectionSetupPayload setupPayload) throws SetupException {
                return new RequestHandler() {
                    @Override
                    public Publisher<Payload> handleRequestResponse(Payload payload) {
                        long time = System.currentTimeMillis();

                        Publisher<Payload> publisher = new Publisher<Payload>() {
                            @Override
                            public void subscribe(Subscriber<? super Payload> s) {
                                Payload responsePayload = new Payload() {
                                    ByteBuffer data = ByteBuffer.wrap(response);
                                    ByteBuffer metadata = ByteBuffer.allocate(0);

                                    public ByteBuffer getData() {
                                        return data;
                                    }

                                    @Override
                                    public ByteBuffer getMetadata() {
                                        return metadata;
                                    }
                                };

                                s.onNext(responsePayload);

                                long diff = System.currentTimeMillis() - time;
                                //timer.update(diff, TimeUnit.NANOSECONDS);
                                s.onComplete();
                            }
                        };

                        return publisher;
                    }

                    @Override
                    public Publisher<Payload> handleRequestStream(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Payload> handleSubscription(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Void> handleFireAndForget(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Payload> handleChannel(Publisher<Payload> payloads) {
                        return null;
                    }

                    @Override
                    public Publisher<Void> handleMetadataPush(Payload payload) {
                        return null;
                    }
                };
            }
        });
    }

}
