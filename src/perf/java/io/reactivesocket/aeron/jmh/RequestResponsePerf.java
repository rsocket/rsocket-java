package io.reactivesocket.aeron.jmh;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.aeron.ReactiveSocketAeronServer;
import io.reactivesocket.aeron.ReactivesocketAeronClient;
import io.reactivesocket.exceptions.SetupException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RequestResponsePerf {
    static final MediaDriver mediaDriver;

    static {
        MediaDriver.Context ctx = new MediaDriver.Context();
        ctx.dirsDeleteOnStart(true);
        ctx.threadingMode(ThreadingMode.DEDICATED);
        ctx.conductorIdleStrategy(new NoOpIdleStrategy());
        ctx.receiverIdleStrategy(new NoOpIdleStrategy());
        ctx.conductorIdleStrategy(new NoOpIdleStrategy());
        mediaDriver = MediaDriver.launch(ctx);
    }

    @State(Scope.Thread)
    public static class Input {
        ReactiveSocketAeronServer server;
        ReactivesocketAeronClient client;
        Blackhole bh;
        Payload payload;

        @Param({ "100", "10000", "100000", "1000000" })
        //        @Param({ "1000" })
        public int size;

        @Setup
        public void init(Blackhole bh) {
            this.bh = bh;

            payload = new Payload() {
                @Override
                public ByteBuffer getData() {
                    return ByteBuffer.wrap("1".getBytes());
                }

                @Override
                public ByteBuffer getMetadata() {
                    return ByteBuffer.allocate(0);
                }
            };

            server = ReactiveSocketAeronServer.create(new ConnectionSetupHandler() {
                @Override
                public RequestHandler apply(ConnectionSetupPayload setupPayload) throws SetupException {
                    return new RequestHandler() {
                        @Override
                        public Publisher<Payload> handleRequestResponse(Payload p) {
                            return new Publisher<Payload>() {
                                @Override
                                public void subscribe(Subscriber<? super Payload> s) {
                                    s.onNext(payload);
                                    s.onComplete();
                                }
                            };
                        }

                        @Override
                        public Publisher<Payload> handleRequestStream(Payload payload) {
                            return null;
                        }

                        @Override
                        public Publisher<Payload> handleSubscription(Payload payload) {
                            return null;
                        }

                        @Override
                        public Publisher<Void> handleFireAndForget(Payload payload) {
                            return null;
                        }

                        @Override
                        public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads) {
                            return null;
                        }

                        @Override
                        public Publisher<Void> handleMetadataPush(Payload payload) {
                            return null;
                        }
                    };
                }
            });

            client = ReactivesocketAeronClient.create("localhost", "localhost");
        }
    }

    @Benchmark
    public void requestReponsePerf(Input input) {
        for (int i = 0; i < input.size; i++) {
            input.client.requestResponse(input.payload).subscribe(new Subscriber<Payload>() {
                @Override
                public void onSubscribe(Subscription s) {

                }

                @Override
                public void onNext(Payload payload) {
                    input.bh.consume(payload);
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onComplete() {

                }
            });
        }
    }
}
