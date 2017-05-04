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

package io.rsocket.transport.netty;

import io.rsocket.lease.DisabledLeaseAcceptingSocket;
import io.rsocket.server.ReactiveSocketServer;
import io.rsocket.test.ClientSetupRule;
import io.rsocket.test.TestReactiveSocket;
import io.rsocket.transport.netty.client.WebsocketTransportClient;
import io.rsocket.transport.netty.server.WebsocketTransportServer;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.server.HttpServer;

import java.net.InetSocketAddress;

public class WebsocketClientSetupRule extends ClientSetupRule {

    public WebsocketClientSetupRule() {
        super(address -> WebsocketTransportClient.create(HttpClient.create(((InetSocketAddress)address).getPort())), () -> {
            return ReactiveSocketServer.create(WebsocketTransportServer.create(HttpServer.create(0)))
                .start((setup, sendingSocket) -> {
                    return new DisabledLeaseAcceptingSocket(new TestReactiveSocket());
                })
                .getServerAddress();
        });
    }

}
