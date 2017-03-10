package com.rolandkuhn.rs;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.client.SetupProvider;
import io.reactivesocket.frame.ByteBufferUtil;
import io.reactivesocket.transport.netty.client.TcpTransportClient;
import io.reactivesocket.util.PayloadImpl;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;

public class Client {
    public static class Performance {
        final String url;
        final int count;
        final double avgSize;

        public Performance(String url, int count, double avgSize) {
            super();
            this.url = url;
            this.count = count;
            this.avgSize = avgSize;
        }

        public String getUrl() {
            return url;
        }

        public int getCount() {
            return count;
        }

        public double getAvgSize() {
            return avgSize;
        }

        @Override
        public String toString() {
            return "Performance [url=" + url + ", count=" + count + ", avgSize=" + avgSize + "]";
        }


    }

    public static Flux<Performance> subscribe(ReactiveSocket socket, String request) {
        return
            socket
                .requestStream(new PayloadImpl(request))
                .publishOn(Schedulers.single())
                .map(payload -> payload.getData())
                .map(ByteBufferUtil::toUtf8String)
                .buffer(Duration.ofSeconds(1))
                .map(l -> {
                double avgSize = l
                    .stream()
                    .mapToInt(String::length)
                    .average()
                    .orElse(0.0);

                return new Performance(request, l.size(), avgSize);
            });
    }

    public static void main(String[] args) {
        int port = 9000;
        String host = "localhost";

        SocketAddress address = new InetSocketAddress(host, port);
        TcpClient client = TcpClient.create(port);
        ReactiveSocket socket = Mono.from(ReactiveSocketClient.create(TcpTransportClient.create(client),
            SetupProvider.keepAlive(KeepAliveProvider.never()).disableLease()).connect()).block();

        for (int i = 0; i < 1; i++) {
            subscribe(socket, "localhost:4096:Object" + i)
                .doOnNext(System.out::println)
                .blockLast();
        }
    }
}