/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.transport.tcp;

import io.reactivesocket.Payload;
import io.reactivesocket.test.TestUtil;
import org.junit.Rule;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;

import java.util.concurrent.TimeUnit;

import static io.reactivesocket.test.TestUtil.*;
import static rx.RxReactiveStreams.*;

public class ClientServerTest {

    @Rule
    public final ClientSetupRule setup = new ClientSetupRule();

    @Test(timeout = 60000)
    public void testRequestResponse1() {
        requestResponseN(1500, 1);
    }

    @Test(timeout = 60000)
    public void testRequestResponse10() {
        requestResponseN(1500, 10);
    }


    @Test(timeout = 60000)
    public void testRequestResponse100() {
        requestResponseN(1500, 100);
    }

    @Test(timeout = 60000)
    public void testRequestResponse10_000() {
        requestResponseN(60_000, 10_000);
    }

    @Test(timeout = 60000)
    public void testRequestStream() {
        TestSubscriber<Payload> ts = TestSubscriber.create();

        toObservable(setup.getReactiveSocket().requestStream(utf8EncodedPayload("hello", "metadata")))
                .subscribe(ts);


        ts.awaitTerminalEvent(3_000, TimeUnit.MILLISECONDS);
        ts.assertValueCount(10);
        ts.assertNoErrors();
        ts.assertCompleted();
    }

    @Test(timeout = 60000)
    public void testRequestSubscription() throws InterruptedException {
        TestSubscriber<Payload> ts = TestSubscriber.create();

        toObservable(setup.getReactiveSocket().requestSubscription(utf8EncodedPayload("hello sub", "metadata sub")))
                .take(10)
                .subscribe(ts);

        ts.awaitTerminalEvent(3_000, TimeUnit.MILLISECONDS);
        ts.assertValueCount(10);
        ts.assertNoErrors();
    }


    public void requestResponseN(int timeout, int count) {

        TestSubscriber<String> ts = TestSubscriber.create();

        Observable
                .range(1, count)
                .flatMap(i ->
                                 toObservable(setup.getReactiveSocket().requestResponse(utf8EncodedPayload("hello", "metadata")))
                                         .map(payload -> byteToString(payload.getData()))
                )
                .doOnError(Throwable::printStackTrace)
                .subscribe(ts);

        ts.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        ts.assertValueCount(count);
        ts.assertNoErrors();
        ts.assertCompleted();
    }


}