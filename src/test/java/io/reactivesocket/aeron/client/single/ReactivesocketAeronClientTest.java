package io.reactivesocket.aeron.client.single;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.aeron.TestUtil;
import io.reactivesocket.aeron.server.ReactiveSocketAeronServer;
import io.reactivesocket.exceptions.SetupException;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

@Ignore
public class ReactivesocketAeronClientTest {

    @BeforeClass
    public static void init() {
        final MediaDriver.Context context = new MediaDriver.Context();
        context.dirsDeleteOnStart(true);

        final MediaDriver mediaDriver = MediaDriver.launch(context);
    }

    @Test
    public void testReconnect() throws Exception {
        ReactiveSocketAeronServer.create(new ConnectionSetupHandler() {
            @Override
            public RequestHandler apply(ConnectionSetupPayload setupPayload) throws SetupException {
                return new RequestHandler() {
                    Frame frame = Frame.from(ByteBuffer.allocate(1));

                    @Override
                    public Publisher<Payload> handleRequestResponse(Payload payload) {
                        String request = TestUtil.byteToString(payload.getData());
                        //System.out.println("Server got => " + request);
                        Observable<Payload> pong = Observable.just(TestUtil.utf8EncodedPayload("pong", null));
                        return RxReactiveStreams.toPublisher(pong);
                    }

                    @Override
                    public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads) {
                        return null;
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
                    public Publisher<Void> handleMetadataPush(Payload payload) {
                        return null;
                    }
                };
            }
        });

        CountDownLatch latch = new CountDownLatch(1);

        ReactivesocketAeronClient client = ReactivesocketAeronClient.create("localhost", "localhost");

        Observable
            .range(1, 1)
            .flatMap(i -> {
                    //System.out.println("pinging => " + i);
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
                    //System.out.println(s + " countdown => " + latch.getCount());
                    latch.countDown();
                }
            });

        latch.await();

        int serverSessionId = client.serverSessionId;
        int sessionId = client.sessionId;

        Assert.assertNotNull(client.connections.get(serverSessionId));

        client.close();

        Assert.assertNull(client.connections.get(serverSessionId));
        Assert.assertNull(client.establishConnectionLatches.get(sessionId));

        ReactivesocketAeronClient newConnection = ReactivesocketAeronClient.create("localhost", "localhost");

    }
}