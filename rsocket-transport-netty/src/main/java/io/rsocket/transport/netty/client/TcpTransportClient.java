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

package io.rsocket.transport.netty.client;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.TransportClient;
import io.rsocket.transport.netty.RSocketLengthCodec;
import io.rsocket.transport.netty.NettyDuplexConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpClient;

public class TcpTransportClient implements TransportClient {
    private final Logger logger = LoggerFactory.getLogger(TcpTransportClient.class);
    private final TcpClient client;

    private TcpTransportClient(TcpClient client) {
        this.client = client;
    }

    public static TcpTransportClient create(TcpClient client) {
        return new TcpTransportClient(client);
    }

    @Override
    public Mono<DuplexConnection> connect() {
        return Mono.create(sink ->
            client.newHandler((in, out) -> {
                in.context().addHandler("client-length-codec", new RSocketLengthCodec());
                NettyDuplexConnection connection = new NettyDuplexConnection(in, out, in.context());
                sink.success(connection);
                return connection.onClose();
            })
            .doOnError(sink::error)
            .subscribe()
        );
    }
}
