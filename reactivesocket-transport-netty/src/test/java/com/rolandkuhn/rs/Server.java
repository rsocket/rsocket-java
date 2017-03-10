package com.rolandkuhn.rs;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.frame.ByteBufferUtil;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.netty.server.TcpTransportServer;
import io.reactivesocket.util.PayloadImpl;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class Server {

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
                        String[] request = ByteBufferUtil.toUtf8String(p.getData()).split(":");
                        int size = Integer.parseInt(request[1]);

                        System.out.println("Got request for " + request);

                        final byte[] buff = new byte[size];
                        Arrays.fill(buff, (byte)65);

                        return Flux.generate(emitter -> emitter.next(new PayloadImpl(buff)));
                    }
                });
            });

        new BufferedReader(new InputStreamReader(System.in)).readLine();
    }
}