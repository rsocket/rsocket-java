package io.reactivesocket.client;

import io.reactivesocket.Payload;
import io.reactivesocket.client.exception.TimeoutException;
import io.reactivesocket.client.filter.TimeoutSocket;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class TimeoutFactoryTest {
    @Test
    public void testTimeoutSocket() {
        TestingReactiveSocket socket = new TestingReactiveSocket(payload -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return payload;
        });
        TimeoutSocket timeout = new TimeoutSocket(socket, 50, TimeUnit.MILLISECONDS);

        timeout.requestResponse(new Payload() {
            @Override
            public ByteBuffer getData() {
                return null;
            }

            @Override
            public ByteBuffer getMetadata() {
                return null;
            }
        }).subscribe(new Subscriber<Payload>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(Payload payload) {
                Assert.assertTrue(false);
            }

            @Override
            public void onError(Throwable t) {
                Assert.assertTrue(t instanceof TimeoutException);
            }

            @Override
            public void onComplete() {
                Assert.assertTrue(false);
            }
        });
    }
}