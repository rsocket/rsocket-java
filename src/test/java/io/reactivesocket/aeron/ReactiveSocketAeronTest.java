package io.reactivesocket.aeron;

import io.reactivesocket.RequestHandler;
import org.junit.Test;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import uk.co.real_logic.aeron.driver.MediaDriver;

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

        ReactivesocketAeronClient client = ReactivesocketAeronClient.create("localhost");
        //Publisher<String> ping = client.requestResponse("ping");
        //RxReactiveStreams.toObservable(ping).doOnError(Throwable::printStackTrace).forEach(a -> System.out.println("pong from the server => " + a));

    }


}
