package io.reactivesocket.tckdrivers.test;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.internal.PublisherUtils;
import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer;
import io.reactivesocket.util.PayloadImpl;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Iterator;

public class SimpleServer {

    public static void main(String[] args) {

        TcpReactiveSocketServer.create(4567)
                .start((setupPayload, reactiveSocket) -> {
                    // create request handler
                    return new RequestHandler.Builder().withRequestResponse(payload -> s -> { // this is a simple echo response
                        s.onSubscribe(new Subscription() {
                            @Override
                            public void request(long n) {
                                System.out.println("Data requested " + n);
                                s.onNext(new PayloadImpl("yo", "what's up"));
                                s.onComplete();
                            }

                            @Override
                            public void cancel() {
                                return;
                            }
                        });
                    })
                            // this is a simple channel handler. We have both access to the client's subscriber as well as
                            // it's publisher, which we must define a subscriber for.
                            .withRequestChannel(payload -> s -> {
                                s.onSubscribe(new Subscription() {
                                    @Override
                                    public void request(long n) {
                                        s.onNext(new PayloadImpl("1", "1"));
                                        s.onNext(new PayloadImpl("2", "2"));
                                        s.onNext(new PayloadImpl("3", "3"));
                                    }

                                    @Override
                                    public void cancel() {
                                        return;
                                    }
                                });
                                payload.subscribe(new Subscriber<Payload>() {
                                    @Override
                                    public void onSubscribe(Subscription s) {
                                        s.request(1000);
                                    }

                                    @Override
                                    public void onNext(Payload payload) {
                                        System.out.println(payload);
                                    }

                                    @Override
                                    public void onError(Throwable t) {

                                    }

                                    @Override
                                    public void onComplete() {
                                        System.out.println("Completed");
                                    }
                                });
                            })
                            .withRequestStream(payload -> s -> {
                                s.onSubscribe(new Subscription() {
                                    @Override
                                    public void request(long n) {
                                        System.out.println("stream requested " + n);
                                        s.onNext(new PayloadImpl("asdfadf", "sdfdsfs"));
                                    }

                                    @Override
                                    public void cancel() {

                                    }
                                });
                            })
                            .build();
                }).awaitShutdown();

    }

}
