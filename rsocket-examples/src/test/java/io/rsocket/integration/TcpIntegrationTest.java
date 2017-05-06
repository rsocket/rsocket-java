package io.rsocket.integration;

import com.google.common.collect.Lists;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.client.KeepAliveProvider;
import io.rsocket.client.RSocketClient;
import io.rsocket.client.SetupProvider;
import io.rsocket.lease.DisabledLeaseAcceptingSocket;
import io.rsocket.server.RSocketServer;
import io.rsocket.transport.TransportServer;
import io.rsocket.transport.netty.client.TcpTransportClient;
import io.rsocket.transport.netty.server.TcpTransportServer;
import io.rsocket.util.PayloadImpl;
import io.rsocket.util.RSocketProxy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TcpIntegrationTest {
    private AbstractRSocket handler = new AbstractRSocket() {
    };

    private TransportServer.StartedServer server;

    @Before
    public void startup() {
        server = RSocketServer.create(
                TcpTransportServer.create(TcpServer.create()))
                .start((setup, sendingSocket) -> new DisabledLeaseAcceptingSocket(new RSocketProxy(handler)));
    }

    private RSocket buildClient() {
        return RSocketClient.create(
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
        handler = new AbstractRSocket() {
            @Override
            public Flux<Payload> requestStream(Payload payload) {
                return Flux.empty();
            }
        };

        RSocket client = buildClient();

        Boolean hasElements = client.requestStream(new PayloadImpl("REQUEST", "META")).hasElements().block();

        assertFalse(hasElements);
    }

    @Test(timeout = 2_000L)
    public void testSingleStream() throws InterruptedException {
        handler = new AbstractRSocket() {
            @Override
            public Flux<Payload> requestStream(Payload payload) {
                return Flux.just(new PayloadImpl("RESPONSE", "METADATA"));
            }
        };

        RSocket client = buildClient();

        Payload result = client.requestStream(new PayloadImpl("REQUEST", "META")).blockLast();

        assertEquals("RESPONSE", StandardCharsets.UTF_8.decode(result.getData()).toString());
    }

    @Test(timeout = 2_000L)
    public void testZeroPayload() throws InterruptedException {
        handler = new AbstractRSocket() {
            @Override
            public Flux<Payload> requestStream(Payload payload) {
                return Flux.just(PayloadImpl.EMPTY);
            }
        };

        RSocket client = buildClient();

        Payload result = client.requestStream(new PayloadImpl("REQUEST", "META")).blockFirst();

        assertEquals("", StandardCharsets.UTF_8.decode(result.getData()).toString());
    }

    @Test(timeout = 2_000L)
    public void testRequestResponseErrors() throws InterruptedException {
        handler = new AbstractRSocket() {
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

        RSocket client = buildClient();

        Payload response1 = client.requestResponse(new PayloadImpl("REQUEST", "META")).onErrorReturn(new PayloadImpl("ERROR")).block();
        Payload response2 = client.requestResponse(new PayloadImpl("REQUEST", "META")).onErrorReturn(new PayloadImpl("ERROR")).block();

        assertEquals("ERROR", StandardCharsets.UTF_8.decode(response1.getData()).toString());
        assertEquals("SUCCESS", StandardCharsets.UTF_8.decode(response2.getData()).toString());
    }


    @Test(timeout = 2_000L)
    public void testTwoConcurrentStreams() throws InterruptedException {
        // Two concurrent streams
        // Stream 1 is a single response
        // Stream 2 is a single response, then completed by the server
        // Stream 1 is then cancelled by the client

        Queue<DirectProcessor<Payload>> streamResults = Lists.newLinkedList();
        DirectProcessor<Payload> processor1 = DirectProcessor.create();
        streamResults.add(processor1);
        DirectProcessor<Payload> processor2 = DirectProcessor.create();
        streamResults.add(processor2);

        handler = new AbstractRSocket() {
            @Override
            public Flux<Payload> requestStream(Payload payload) {
                return streamResults.remove();
            }
        };

        RSocket client = buildClient();

        Flux<Payload> response1 = client.requestStream(new PayloadImpl("REQUEST1"));
        Flux<Payload> response2 = client.requestStream(new PayloadImpl("REQUEST2"));

        processor1.onNext(new PayloadImpl("RESPONSE1A"));
        // TODO comment out this line
        processor1.onComplete();
        processor2.onNext(new PayloadImpl("RESPONSE2A"));
        processor2.onComplete();

        Payload r2a = response2.blockFirst();
        Payload r1a = response1.blockFirst();
    }
}
