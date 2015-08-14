package io.reactivesocket.aeron;

import io.reactivesocket.RequestHandler;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.Observable;
import rx.RxReactiveStreams;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.util.concurrent.CountDownLatch;

/**
 * Created by rroeser on 8/14/15.
 */
public class ReactiveSocketAeronTest {
    @Test
    public void test() throws Exception {



        final MediaDriver.Context context = new MediaDriver.Context();
        context.dirsDeleteOnStart();

        final MediaDriver mediaDriver = MediaDriver.launch(context);

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

        CountDownLatch latch = new CountDownLatch(1);

        ReactivesocketAeronClient client = ReactivesocketAeronClient.create("localhost");
        client.requestResponse("ping", "ping metadata").subscribe(new Subscriber<String>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
                System.out.println("here we go");
            }

            @Override
            public void onNext(String s) {
                System.out.println(s);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        latch.await();
    }


}
