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
package io.reactivesocket.javax.websocket.client;

import io.reactivesocket.Frame;
import rx.Observable;
import rx.subjects.PublishSubject;

import javax.websocket.*;
import java.nio.ByteBuffer;

public class ReactiveSocketWebSocketClient extends Endpoint {
    private final PublishSubject<Frame> input;

    public ReactiveSocketWebSocketClient() {
        this.input = PublishSubject.create();
    }

    public Observable<Frame> getInput() {
        return input;
    }

    @Override
    public void onOpen(Session session, EndpointConfig config) {
        session.addMessageHandler(new MessageHandler.Whole<ByteBuffer>() {
            @Override
            public void onMessage(ByteBuffer message) {
                Frame frame = Frame.from(message);
                input.onNext(frame);
            }
        });
    }

    @Override
    public void onClose(Session session, CloseReason closeReason) {
        input.onCompleted();
    }

    @Override
    public void onError(Session session, Throwable thr) {
        input.onError(thr);
    }
}
