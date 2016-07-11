/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.client;

import io.reactivesocket.Payload;
import io.reactivesocket.exceptions.TimeoutException;
import io.reactivesocket.client.filter.TimeoutSocket;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;

public class TimeoutFactoryTest {
    @Test
    public void testTimeoutSocket() {
        TestingReactiveSocket socket = new TestingReactiveSocket((subscriber, payload) -> {return false;});
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
                throw new AssertionError("onNext invoked when not expected.");
            }

            @Override
            public void onError(Throwable t) {
                MatcherAssert.assertThat("Unexpected exception in onError", t, instanceOf(TimeoutException.class));
            }

            @Override
            public void onComplete() {
                throw new AssertionError("onComplete invoked when not expected.");
            }
        });
    }
}