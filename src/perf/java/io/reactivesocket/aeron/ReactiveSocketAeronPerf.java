package io.reactivesocket.aeron;

import io.reactivesocket.RequestHandler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.Observable;
import rx.RxReactiveStreams;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ReactiveSocketAeronPerf {
     //static final MediaDriver mediaDriver = MediaDriver.launchEmbedded();

    static final ReactiveSocketAeronServer server = ReactiveSocketAeronServer.create(new RequestHandler() {
        @Override
        public Publisher<String> handleRequestResponse(String request) {
            return RxReactiveStreams.toPublisher(Observable.just(request));
        }

        @Override
        public Publisher<String> handleRequestStream(String request) {
            return null;
        }

        @Override
        public Publisher<String> handleRequestSubscription(String request) {
            return null;
        }

        @Override
        public Publisher<Void> handleFireAndForget(String request) {
            return null;
        }
    });

    static final ReactivesocketAeronClient client = ReactivesocketAeronClient.create("localhost");

    @State(Scope.Benchmark)
    public static class Input {
        private String message;
        private String metaData;
        private Blackhole bh;

        @Setup
        public void setup(Blackhole bh) {
            this.bh = bh;
            this.message = "ping";
            this.metaData = "metadata test";
        }

        public Blackhole getBh() {
            return bh;
        }

        public String getMetaData() {
            return metaData;
        }

        public String getMessage() {
            return message;
        }

        public Subscriber<String> newSubscriber() {
            return new Subscriber<String>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(8);
                }

                @Override
                public void onNext(String s) {
                    bh.consume(s);
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onComplete() {

                }
            };
        }
    }

    @Benchmark
    public void pingPongTest(Input input) {
        client
            .requestResponse(input.getMessage(), input.getMetaData())
            .subscribe(input.newSubscriber());
    }
}
