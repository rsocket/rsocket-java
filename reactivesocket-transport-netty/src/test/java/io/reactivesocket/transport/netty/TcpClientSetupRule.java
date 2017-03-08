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

package io.reactivesocket.transport.netty;

import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.test.ClientSetupRule;
import io.reactivesocket.test.TestReactiveSocket;
import io.reactivesocket.transport.netty.client.TcpTransportClient;
import io.reactivesocket.transport.netty.server.TcpTransportServer;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;

import java.net.InetSocketAddress;

public class TcpClientSetupRule extends ClientSetupRule {

    public TcpClientSetupRule() {
        super(address -> TcpTransportClient.create(TcpClient.create(options -> options.connect((InetSocketAddress) address))), () -> {
            return ReactiveSocketServer.create(TcpTransportServer.create(TcpServer.create()))
                .start((setup, sendingSocket) -> {
                    return new DisabledLeaseAcceptingSocket(new TestReactiveSocket());
                })
                .getServerAddress();
        });
    }

}
