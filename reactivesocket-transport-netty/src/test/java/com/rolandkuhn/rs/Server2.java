package com.rolandkuhn.rs;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.netty.server.TcpTransportServer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Server2 {

    public static void main(String[] args) throws IOException {
        int port = 9000;

        TcpServer server = TcpServer.create(port);
        ReactiveSocketServer.create(TcpTransportServer.create(server))
            .start((setupPayload, reactiveSocket) -> {
                return new DisabledLeaseAcceptingSocket(new AbstractReactiveSocket() {
                    @Override
                    public Mono<Payload> requestResponse(Payload p) {
                        return Mono.just(p);
                    }

                    @Override
                    public Flux<Payload> requestStream(Payload p) {
                        return Flux.error(new RuntimeException("boom"));
                    }
                });
            });

        new BufferedReader(new InputStreamReader(System.in)).readLine();
    }
}