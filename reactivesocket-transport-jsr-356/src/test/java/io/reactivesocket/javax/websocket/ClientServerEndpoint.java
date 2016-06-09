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
package io.reactivesocket.javax.websocket;

import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.javax.websocket.server.ReactiveSocketWebSocketServer;
import io.reactivesocket.test.TestUtil;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.RxReactiveStreams;

public class ClientServerEndpoint extends ReactiveSocketWebSocketServer {
    public ClientServerEndpoint() {
        super((setupPayload, rs) -> new RequestHandler() {
            @Override
            public Publisher<Payload> handleRequestResponse(Payload payload) {
                return s -> {
                    //System.out.println("Handling request/response payload => " + s.toString());
                    Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");
                    s.onNext(response);
                    s.onComplete();
                };
            }

            @Override
            public Publisher<Payload> handleRequestStream(Payload payload) {
                Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");

                return RxReactiveStreams
                    .toPublisher(Observable
                        .range(1, 10)
                        .map(i -> response));
            }

            @Override
            public Publisher<Payload> handleSubscription(Payload payload) {
                Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");

                return RxReactiveStreams
                    .toPublisher(Observable
                        .range(1, 10)
                        .map(i -> response)
                        .repeat());
            }

            @Override
            public Publisher<Void> handleFireAndForget(Payload payload) {
                return Subscriber::onComplete;
            }

            @Override
            public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> inputs) {
                return null;
            }

            @Override
            public Publisher<Void> handleMetadataPush(Payload payload) {
                return null;
            }
        });
    }
}
