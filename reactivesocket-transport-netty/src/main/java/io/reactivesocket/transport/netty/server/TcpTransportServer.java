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

package io.reactivesocket.transport.netty.server;

import io.reactivesocket.transport.TransportServer;
import io.reactivesocket.transport.netty.ReactiveSocketLengthCodec;
import io.reactivesocket.transport.netty.NettyDuplexConnection;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.tcp.TcpServer;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

public class TcpTransportServer implements TransportServer {
    TcpServer server;

    public TcpTransportServer(TcpServer server) {
        this.server = server;
    }

    public static TcpTransportServer create(TcpServer server) {
        return new TcpTransportServer(server);
    }

    @Override
    public StartedServer start(ConnectionAcceptor acceptor) {
        NettyContext context = server.newHandler((in, out) -> {
            in.context().addHandler("server-length-codec", new ReactiveSocketLengthCodec());
            NettyDuplexConnection connection = new NettyDuplexConnection(in, out, in.context());
             acceptor.apply(connection).subscribe();

             return out.neverComplete();
        }).block();

        return new StartServerImpl(context);
    }

    static class StartServerImpl implements StartedServer {
        NettyContext context;

        StartServerImpl(NettyContext context) {
            this.context = context;
        }

        @Override
        public SocketAddress getServerAddress() {
            return context.address();
        }

        @Override
        public int getServerPort() {
            return context.address().getPort();
        }

        @Override
        public void awaitShutdown() {
            context.onClose().block();
        }

        @Override
        public void awaitShutdown(long duration, TimeUnit durationUnit) {
            context.onClose().blockMillis(TimeUnit.MILLISECONDS.convert(duration, durationUnit));
        }

        @Override
        public void shutdown() {
            context.dispose();
        }
    }

}