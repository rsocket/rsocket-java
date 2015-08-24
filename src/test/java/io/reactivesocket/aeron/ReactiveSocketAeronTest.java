package io.reactivesocket.aeron;

import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rroeser on 8/14/15.
 */
@Ignore
public class ReactiveSocketAeronTest {
    @BeforeClass
    public static void init() {
        final MediaDriver.Context context = new MediaDriver.Context();
        context.dirsDeleteOnStart();

        final MediaDriver mediaDriver = MediaDriver.launch(context);
    }

    @Test(timeout = 70000)
    public void testRequestReponse() throws Exception {
        ReactiveSocketAeronServer.create(new RequestHandler() {
            Frame frame = Frame.from(ByteBuffer.allocate(1));

            @Override
            public Publisher<Payload> handleRequestResponse(Payload payload) {
                String request = TestUtil.byteToString(payload.getData());
                System.out.println("Server got => " + request);
                Observable<Payload> pong = Observable.just(TestUtil.utf8EncodedPayload("pong", null));
                return RxReactiveStreams.toPublisher(pong);
            }

            @Override
            public Publisher<Payload> handleRequestStream(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> handleRequestSubscription(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Void> handleFireAndForget(Payload payload) {
                return null;
            }
        });

        CountDownLatch latch = new CountDownLatch(10_000);


        ReactivesocketAeronClient client = ReactivesocketAeronClient.create("localhost");

        Observable
            .range(1, 10_000)
            .flatMap(i -> {
                    System.out.println("pinging => " + i);
                    Payload payload = TestUtil.utf8EncodedPayload("ping =>" + i, null);
                    return  RxReactiveStreams.toObservable(client.requestResponse(payload));
                }
            )
            .subscribe(new rx.Subscriber<Payload>() {
                @Override
                public void onCompleted() {
                }

                @Override
                public void onError(Throwable e) {
                    e.printStackTrace();
                }

                @Override
                public void onNext(Payload s) {
                    System.out.println(s + " countdown => " + latch.getCount());
                    latch.countDown();
                }
            });

        latch.await();
    }

    @Test(timeout = 60000)
    public void sendLargeMessage() throws Exception {

        Random random = new Random();
        byte[] b = new byte[1_000_000];
        random.nextBytes(b);

        ReactiveSocketAeronServer.create(new RequestHandler() {
            @Override
            public Publisher<Payload> handleRequestResponse(Payload payload) {
                System.out.println("Server got => " + b.length);
                Observable<Payload> pong = Observable.just(TestUtil.utf8EncodedPayload("pong", null));
                return RxReactiveStreams.toPublisher(pong);
            }

            @Override
            public Publisher<Payload> handleRequestStream(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Payload> handleRequestSubscription(Payload payload) {
                return null;
            }

            @Override
            public Publisher<Void> handleFireAndForget(Payload payload) {
                return null;
            }
        });

        CountDownLatch latch = new CountDownLatch(2);


        ReactivesocketAeronClient client = ReactivesocketAeronClient.create("localhost");

        Observable
            .range(1, 2)
            .flatMap(i -> {
                    System.out.println("pinging => " + i);
                    Payload payload = new Payload() {
                        @Override
                        public ByteBuffer getData() {
                            return ByteBuffer.wrap(b);
                        }

                        @Override
                        public ByteBuffer getMetadata() {
                            return null;
                        }
                    };
                    return  RxReactiveStreams.toObservable(client.requestResponse(payload));
                }
            )
            .subscribe(new rx.Subscriber<Payload>() {
                @Override
                public void onCompleted() {
                }

                @Override
                public void onError(Throwable e) {
                    e.printStackTrace();
                }

                @Override
                public void onNext(Payload s) {
                    System.out.println(s + " countdown => " + latch.getCount());
                    latch.countDown();
                }
            });

        latch.await();
    }

}
