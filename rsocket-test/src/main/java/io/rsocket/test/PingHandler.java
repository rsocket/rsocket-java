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

package io.rsocket.test;

import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.lease.DisabledLeaseAcceptingSocket;
import io.rsocket.lease.LeaseEnforcingSocket;
import io.rsocket.server.RSocketServer.SocketAcceptor;
import io.rsocket.util.PayloadImpl;
import reactor.core.publisher.Mono;

import java.util.concurrent.ThreadLocalRandom;

public class PingHandler implements SocketAcceptor {

    private final Payload pong;

    public PingHandler() {
        byte[] data = new byte[1024];
        ThreadLocalRandom.current().nextBytes(data);
        pong = new PayloadImpl(data);
    }

    public PingHandler(byte[] data) {
        pong = new PayloadImpl(data);
    }

    @Override
    public LeaseEnforcingSocket accept(ConnectionSetupPayload setupPayload, RSocket reactiveSocket) {
        return new DisabledLeaseAcceptingSocket(new AbstractRSocket() {
            @Override
            public Mono<Payload> requestResponse(Payload payload) {
                return Mono.just(pong);
            }
        });
    }
}
