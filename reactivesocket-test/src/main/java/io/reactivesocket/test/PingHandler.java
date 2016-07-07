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

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.exceptions.SetupException;
import io.reactivesocket.util.PayloadImpl;
import rx.Observable;
import rx.RxReactiveStreams;

import java.util.concurrent.ThreadLocalRandom;

public class PingHandler implements ConnectionSetupHandler {

    private final byte[] pong;

    public PingHandler() {
        pong = new byte[1024];
        ThreadLocalRandom.current().nextBytes(pong);
    }

    public PingHandler(byte[] pong) {
        this.pong = pong;
    }

    @Override
    public RequestHandler apply(ConnectionSetupPayload setupPayload, ReactiveSocket reactiveSocket)
            throws SetupException {
        return new RequestHandler.Builder()
                .withRequestResponse(payload -> {
                    Payload responsePayload = new PayloadImpl(pong);
                    return RxReactiveStreams.toPublisher(Observable.just(responsePayload));
                })
                .build();
    }
}
