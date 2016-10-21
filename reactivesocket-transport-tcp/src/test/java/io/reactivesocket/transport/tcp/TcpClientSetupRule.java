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

package io.reactivesocket.transport.tcp;

import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.test.ClientSetupRule;
import io.reactivesocket.test.TestReactiveSocket;
import io.reactivesocket.transport.tcp.client.TcpTransportClient;
import io.reactivesocket.transport.tcp.server.TcpTransportServer;

import static io.netty.handler.logging.LogLevel.DEBUG;

public class TcpClientSetupRule extends ClientSetupRule {

    public TcpClientSetupRule() {
        super(TcpTransportClient::create, () -> {
            return ReactiveSocketServer.create(TcpTransportServer.create(0)
                .configureServer(server -> server.enableWireLogging("test-server", DEBUG)))
                .start((setup, sendingSocket) -> {
                    return new DisabledLeaseAcceptingSocket(new TestReactiveSocket());
                })
                .getServerAddress();
        });
    }

}
