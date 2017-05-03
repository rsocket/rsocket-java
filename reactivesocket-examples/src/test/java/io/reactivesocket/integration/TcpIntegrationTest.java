package io.reactivesocket.integration;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.client.SetupProvider;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.TransportServer;
import io.reactivesocket.transport.netty.client.TcpTransportClient;
import io.reactivesocket.transport.netty.server.TcpTransportServer;
import io.reactivesocket.util.PayloadImpl;
import io.reactivesocket.util.ReactiveSocketProxy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TcpIntegrationTest {
    private AbstractReactiveSocket handler = new AbstractReactiveSocket() {
    };

    private TransportServer.StartedServer server;

    @Before
    public void startup() {
        server = ReactiveSocketServer.create(
                TcpTransportServer.create(TcpServer.create()))
                .start((setup, sendingSocket) -> new DisabledLeaseAcceptingSocket(new ReactiveSocketProxy(handler)));
    }

    private ReactiveSocket buildClient() {
        return ReactiveSocketClient.create(
                TcpTransportClient.create(TcpClient.create(server.getServerPort())),
                SetupProvider.keepAlive(KeepAliveProvider.never()).disableLease())
                .connect().block();
    }

    @After
    public void cleanup() {
        server.shutdown();
    }

    @Test(timeout = 2_000L)
    public void testCompleteWithoutNext() throws InterruptedException {
        handler = new AbstractReactiveSocket() {
            @Override
            public Flux<Payload> requestStream(Payload payload) {
                return Flux.empty();
            }
        };

        ReactiveSocket client = buildClient();

        Boolean hasElements = client.requestStream(new PayloadImpl("REQUEST", "META")).hasElements().block();

        assertFalse(hasElements);
    }

    @Test(timeout = 2_000L)
    public void testSingleStream() throws InterruptedException {
        handler = new AbstractReactiveSocket() {
            @Override
            public Flux<Payload> requestStream(Payload payload) {
                return Flux.just(new PayloadImpl("RESPONSE", "METADATA"));
            }
        };

        ReactiveSocket client = buildClient();

        Payload result = client.requestStream(new PayloadImpl("REQUEST", "META")).blockLast();

        assertEquals("RESPONSE", StandardCharsets.UTF_8.decode(result.getData()).toString());
    }

    @Test(timeout = 2_000L)
    public void testZeroPayload() throws InterruptedException {
        handler = new AbstractReactiveSocket() {
            @Override
            public Flux<Payload> requestStream(Payload payload) {
                return Flux.just(PayloadImpl.EMPTY);
            }
        };

        ReactiveSocket client = buildClient();

        Payload result = client.requestStream(new PayloadImpl("REQUEST", "META")).blockFirst();

        assertEquals("", StandardCharsets.UTF_8.decode(result.getData()).toString());
    }

    @Test(timeout = 2_000L)
    public void testRequestResponseErrors() throws InterruptedException {
        handler = new AbstractReactiveSocket() {
            boolean first = true;

            @Override
            public Mono<Payload> requestResponse(Payload payload) {
                if (first) {
                    first = false;
                    return Mono.error(new RuntimeException("EX"));
                } else {
                    return Mono.just(new PayloadImpl("SUCCESS"));
                }
            }
        };

        ReactiveSocket client = buildClient();

        Payload response1 = client.requestResponse(new PayloadImpl("REQUEST", "META")).onErrorReturn(new PayloadImpl("ERROR")).block();
        Payload response2 = client.requestResponse(new PayloadImpl("REQUEST", "META")).onErrorReturn(new PayloadImpl("ERROR")).block();

        assertEquals("ERROR", StandardCharsets.UTF_8.decode(response1.getData()).toString());
        assertEquals("SUCCESS", StandardCharsets.UTF_8.decode(response2.getData()).toString());
    }
}