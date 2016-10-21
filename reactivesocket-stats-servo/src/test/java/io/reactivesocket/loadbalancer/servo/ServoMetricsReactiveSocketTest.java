/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.loadbalancer.servo;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Flowable;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;

public class ServoMetricsReactiveSocketTest {
    @Test
    public void testCountSuccess() {
        ServoMetricsReactiveSocket client = new ServoMetricsReactiveSocket(new RequestResponseSocket(), "test");

        Publisher<Payload> payloadPublisher = client.requestResponse(PayloadImpl.EMPTY);

        TestSubscriber<Payload> subscriber = new TestSubscriber<>();
        Flowable.fromPublisher(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();

        Assert.assertEquals(1, client.success.get());
    }

    @Test
    public void testCountFailure() {
        ServoMetricsReactiveSocket client = new ServoMetricsReactiveSocket(new AbstractReactiveSocket() {}, "test");

        Publisher<Payload> payloadPublisher = client.requestResponse(PayloadImpl.EMPTY);

        TestSubscriber<Payload> subscriber = new TestSubscriber<>();
        Flowable.fromPublisher(payloadPublisher).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertError(UnsupportedOperationException.class);

        Assert.assertEquals(1, client.failure.get());

    }

    @Test
    public void testHistogram() throws Exception {
        ServoMetricsReactiveSocket client = new ServoMetricsReactiveSocket(new RequestResponseSocket(), "test");

        for (int i = 0; i < 10; i ++) {
            Publisher<Payload> payloadPublisher = client.requestResponse(PayloadImpl.EMPTY);

            TestSubscriber<Payload> subscriber = new TestSubscriber<>();
            Flowable.fromPublisher(payloadPublisher).subscribe(subscriber);
            subscriber.awaitTerminalEvent();
            subscriber.assertNoErrors();
        }

        Thread.sleep(3_000);

        System.out.println(client.histrogramToString());

        Assert.assertEquals(10, client.success.get());
        Assert.assertEquals(0, client.failure.get());
        Assert.assertNotEquals(client.timer.getMax(), client.timer.getMin());
    }

    private static class RequestResponseSocket extends AbstractReactiveSocket {
        @Override
        public Publisher<Payload> requestResponse(Payload payload) {
            return Px.just(new PayloadImpl("Test"));
        }
    }
}
