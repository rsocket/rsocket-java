package io.reactivesocket.examples.sampleapp;

import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer;
import io.reactivesocket.util.PayloadImpl;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;


/**
 *
 */
public class SampleServer {

    public static void main(String[] args) {

        TcpReactiveSocketServer.create(4567).start((setupPayload, reactiveSocket) -> {
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
                    // this is a simple stream handler loops through a list and sends each datum to the client
                    .withRequestStream(payload -> s -> {
                        System.out.println(payload);
                        s.onSubscribe(new Subscription() {

                            @Override
                            public void request(long n) {
                                s.onNext(new PayloadImpl("hello", "hello"));
                            }

                            @Override
                            public void cancel() {
                                return;
                            }
                        });
                    })
                    // this is a trivial fire and forget handler that takes in the payload sent by the client and
                    // adds it to an internal list
                    .withFireAndForget(payload -> {
                        System.out.println(payload);
                        return new Publisher<Void>() {
                            @Override
                            public void subscribe(Subscriber<? super Void> s) {

                            }
                        };
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
                    .build();
        }).awaitShutdown();

    }

}
