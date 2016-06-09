/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.netty.tcp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DefaultReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.netty.tcp.client.ClientTcpDuplexConnection;
import io.reactivesocket.netty.tcp.server.ReactiveSocketServerHandler;
import io.reactivesocket.test.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class ClientServerTest {

    static ReactiveSocket client;
    static Channel serverChannel;

    static EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    static EventLoopGroup workerGroup = new NioEventLoopGroup(4);

    static ReactiveSocketServerHandler serverHandler = ReactiveSocketServerHandler.create((setupPayload, rs) ->
        new RequestHandler() {
            @Override
            public Publisher<Payload> handleRequestResponse(Payload payload) {
                return s -> {
                    //System.out.println("Handling request/response payload => " + s.toString());
                    Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");
                    s.onNext(response);
                    s.onComplete();
                };
            }

            @Override
            public Publisher<Payload> handleRequestStream(Payload payload) {
                Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");

                return RxReactiveStreams
                        .toPublisher(Observable
                                .range(1, 10)
                                .map(i -> response));
            }

            @Override
            public Publisher<Payload> handleSubscription(Payload payload) {
                Payload response = TestUtil.utf8EncodedPayload("hello world", "metadata");

                return RxReactiveStreams
                        .toPublisher(Observable
                                .range(1, 10)
                                .map(i -> response)
                                .repeat());
            }

            @Override
            public Publisher<Void> handleFireAndForget(Payload payload) {
                return Subscriber::onComplete;
            }

            @Override
            public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> inputs) {
                return null;
            }

            @Override
            public Publisher<Void> handleMetadataPush(Payload payload) {
                return null;
            }
        }
    );

    @BeforeClass
    public static void setup() throws Exception {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(serverHandler);
                }
            });

        serverChannel = b.bind("localhost", 7878).sync().channel();

        ClientTcpDuplexConnection duplexConnection = RxReactiveStreams.toObservable(
            ClientTcpDuplexConnection.create(InetSocketAddress.createUnresolved("localhost", 7878), new NioEventLoopGroup())
        ).toBlocking().single();

        client = DefaultReactiveSocket
            .fromClientConnection(duplexConnection, ConnectionSetupPayload.create("UTF-8", "UTF-8"), t -> t.printStackTrace());

        client.startAndWait();
    }

    @AfterClass
    public static void tearDown() {
        serverChannel.close();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    @Test
    public void testRequestResponse1() {
        requestResponseN(1500, 1);
    }

    @Test
    public void testRequestResponse10() {
        requestResponseN(1500, 10);
    }


    @Test
    public void testRequestResponse100() {
        requestResponseN(1500, 100);
    }

    @Test
    public void testRequestResponse10_000() {
        requestResponseN(60_000, 10_000);
    }

    @Test
    public void testRequestStream() {
        TestSubscriber<Payload> ts = TestSubscriber.create();

        RxReactiveStreams
            .toObservable(client.requestStream(TestUtil.utf8EncodedPayload("hello", "metadata")))
            .subscribe(ts);


        ts.awaitTerminalEvent(3_000, TimeUnit.MILLISECONDS);
        ts.assertValueCount(10);
        ts.assertNoErrors();
        ts.assertCompleted();
    }

    @Test
    public void testRequestSubscription() throws InterruptedException {
        TestSubscriber<Payload> ts = TestSubscriber.create();

        RxReactiveStreams
            .toObservable(client.requestSubscription(
                TestUtil.utf8EncodedPayload("hello sub", "metadata sub")))
            .take(10)
            .subscribe(ts);

        ts.awaitTerminalEvent(3_000, TimeUnit.MILLISECONDS);
        ts.assertValueCount(10);
        ts.assertNoErrors();
    }


    public void requestResponseN(int timeout, int count) {

        TestSubscriber<String> ts = TestSubscriber.create();

        Observable
            .range(1, count)
            .flatMap(i ->
                RxReactiveStreams
                    .toObservable(client
                        .requestResponse(TestUtil.utf8EncodedPayload("hello", "metadata")))
                    .map(payload -> TestUtil.byteToString(payload.getData()))
            )
            .doOnError(Throwable::printStackTrace)
            .subscribe(ts);

        ts.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        ts.assertValueCount(count);
        ts.assertNoErrors();
        ts.assertCompleted();
    }


}