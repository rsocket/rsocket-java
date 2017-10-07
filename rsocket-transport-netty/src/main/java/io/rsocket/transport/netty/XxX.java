package io.rsocket.transport.netty;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.PayloadImpl;
import reactor.core.publisher.Flux;

import java.net.URI;

public class XxX {
    public static void main(String[] args) {
        WebsocketClientTransport ws = WebsocketClientTransport.create(URI.create("ws://rsocket-demo.herokuapp.com/ws"));
        RSocket client = RSocketFactory.connect().keepAlive().transport(ws).start().block();

        Flux<Payload> s = client.requestStream(PayloadImpl.textPayload("trump"));

        System.out.println("Ready");

        System.out.println(s.take(5).blockLast().getDataUtf8());
        System.out.println(s.take(5).blockLast().getDataUtf8());
    }
}
