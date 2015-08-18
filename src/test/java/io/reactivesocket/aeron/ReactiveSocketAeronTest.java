package io.reactivesocket.aeron;

import io.reactivesocket.RequestHandler;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.util.concurrent.CountDownLatch;

/**
 * Created by rroeser on 8/14/15.
 */
public class ReactiveSocketAeronTest {
    @BeforeClass
    public static void init() {
        final MediaDriver.Context context = new MediaDriver.Context();
        context.dirsDeleteOnStart();

        final MediaDriver mediaDriver = MediaDriver.launch(context);
    }

    @Test(timeout = 5000)
    public void testRequestReponse() throws Exception {
        ReactiveSocketAeronServer.create(new RequestHandler() {
            @Override
            public Publisher<String> handleRequestResponse(String request) {
                System.out.println("Server got => " + request);
                Observable<String> pong = Observable.just("pong");
                return RxReactiveStreams.toPublisher(pong);
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

        CountDownLatch latch = new CountDownLatch(10_000);


        ReactivesocketAeronClient client = ReactivesocketAeronClient.create("localhost");

        Observable
            .range(1, 10_000)
            .flatMap(i ->
                RxReactiveStreams.toObservable(client.requestResponse("ping =>" + i, "ping metadata"))
            )
            .subscribe(new rx.Subscriber<String>() {
                @Override
                public void onCompleted() {
                }

                @Override
                public void onError(Throwable e) {
                    e.printStackTrace();
                }

                @Override
                public void onNext(String s) {
                    System.out.println(s + " countdown => " + latch.getCount());
                    latch.countDown();
                }
            });

        latch.await();
    }

}
