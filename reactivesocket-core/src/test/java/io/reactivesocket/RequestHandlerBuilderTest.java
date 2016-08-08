/**
 * Copyright 2015 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket;

import io.reactivesocket.internal.Publishers;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;

public class RequestHandlerBuilderTest {
    private static final String NO_HANDLER_ERROR_MESSAGE_PATTERN = "No .*? handler";
    TestSubscriber<Payload> payloadTestSubscriber;
    TestSubscriber<Void> voidTestSubscriber;

    private static Payload payload = new Payload() {
        @Override
        public ByteBuffer getData() {
            return Frame.NULL_BYTEBUFFER;
        }

        @Override
        public ByteBuffer getMetadata() {
            return Frame.NULL_BYTEBUFFER;
        }
    };

    @Before
    public void setup() {
        payloadTestSubscriber = new TestSubscriber<>();
        voidTestSubscriber = new TestSubscriber<>();
    }

    @Test
    public void testWithoutHandlers() {
        RequestHandler handler = new RequestHandler.Builder().build();

        // assert that all handlers publish an error event
        assertErrorMessageOnNext(handler.handleRequestResponse(payload), new TestSubscriber<>());
        assertErrorMessageOnNext(handler.handleRequestStream(payload), new TestSubscriber<>());
        assertErrorMessageOnNext(handler.handleChannel(payload, Publishers.just(payload)), new TestSubscriber<>());
        assertErrorMessageOnNext(handler.handleSubscription(payload), new TestSubscriber<>());
        assertOnError(handler.handleFireAndForget(payload), new TestSubscriber<>(), RuntimeException.class);
        assertOnError(handler.handleMetadataPush(payload), new TestSubscriber<>(), RuntimeException.class);
    }

    private static <T> void assertOnError(Publisher<T> p, TestSubscriber<T> t, Class<? extends Exception> exceptionType) {
        p.subscribe(t);
        t.awaitTerminalEvent();
        t.assertError(exceptionType);
    }

    private static <T> void assertErrorMessageOnNext(Publisher<T> p, TestSubscriber<T> t) {
        p.subscribe(t);
        t.awaitTerminalEvent();
        t.assertValueCount(1);
        ByteBuffer data = ((Payload)t.values().get(0)).getData();
        String errorMessage = new String(data.array());
        Assert.assertTrue(errorMessage.matches(NO_HANDLER_ERROR_MESSAGE_PATTERN));
    }

    @Test
    public void testFireAndForget() {
        final Publisher<Void> voidPublisher = Publishers.just(null);
        RequestHandler handler = new RequestHandler.Builder().withFireAndForget(p -> voidPublisher).build();

        // assert that handleFireAndForget has a complete event
        TestSubscriber<Void> fireAndForgetSubscriber = new TestSubscriber<>();
        handler.handleFireAndForget(payload).subscribe(fireAndForgetSubscriber);
        fireAndForgetSubscriber.awaitTerminalEvent();
        fireAndForgetSubscriber.assertComplete();
        fireAndForgetSubscriber.assertNoValues();
        fireAndForgetSubscriber.assertNoErrors();

        // assert that all other handlers publish an error event
        assertErrorMessageOnNext(handler.handleRequestResponse(payload), new TestSubscriber<>());
        assertErrorMessageOnNext(handler.handleRequestStream(payload), new TestSubscriber<>());
        assertErrorMessageOnNext(handler.handleChannel(payload, Publishers.just(payload)), new TestSubscriber<>());
        assertErrorMessageOnNext(handler.handleSubscription(payload), new TestSubscriber<>());
        assertOnError(handler.handleMetadataPush(payload), new TestSubscriber<>(), RuntimeException.class);
    }

    @Test
    public void testWithAllHandlers() {
        final Publisher<Void> fireAndForgetPublisher = Publishers.just(null);
        final Publisher<Void> metadataPushPublisher = Publishers.just(null);
        final Publisher<Payload> requestResponsePublisher = Publishers.just(payload);
        final Publisher<Payload> requestStreamPublisher = Publishers.just(payload);
        final Publisher<Payload> subscriptionPublisher = Publishers.just(payload);
        final Publisher<Payload> channelPublisher = Publishers.just(payload);

        RequestHandler handler = new RequestHandler.Builder()
                .withRequestChannel((p, pP) -> channelPublisher)
                .withFireAndForget(p -> fireAndForgetPublisher)
                .withMetadataPush(p -> metadataPushPublisher)
                .withRequestResponse(p -> requestResponsePublisher)
                .withRequestStream(p -> requestStreamPublisher)
                .withRequestSubscription(p -> subscriptionPublisher)
                .build();

        // assert that handleChannel has a complete event
        TestSubscriber<Payload> payloadSubscriber = new TestSubscriber<>();
        handler.handleChannel(payload, Publishers.just(payload)).subscribe(payloadSubscriber);
        payloadSubscriber.awaitTerminalEvent();
        payloadSubscriber.assertComplete();
        payloadSubscriber.assertValueCount(1);

        // assert that handleFireAndForget has a complete event
        TestSubscriber<Void> voidTestSubscriber = new TestSubscriber<>();
        handler.handleFireAndForget(payload).subscribe(voidTestSubscriber);
        voidTestSubscriber.awaitTerminalEvent();
        voidTestSubscriber.assertComplete();
        voidTestSubscriber.assertNoValues();
        voidTestSubscriber.assertNoErrors();

        // assert that handleMetadataPush has a complete event
        voidTestSubscriber = new TestSubscriber<>();
        handler.handleMetadataPush(payload).subscribe(voidTestSubscriber);
        voidTestSubscriber.awaitTerminalEvent();
        voidTestSubscriber.assertComplete();
        voidTestSubscriber.assertNoValues();
        voidTestSubscriber.assertNoErrors();

        // assert that handleRequestResponse has a complete event
        payloadSubscriber = new TestSubscriber<>();
        handler.handleRequestResponse(payload).subscribe(payloadSubscriber);
        payloadSubscriber.awaitTerminalEvent();
        payloadSubscriber.assertComplete();
        payloadSubscriber.assertValueCount(1);

        // assert that handleRequestStream has a complete event
        payloadSubscriber = new TestSubscriber<>();
        handler.handleRequestStream(payload).subscribe(payloadSubscriber);
        payloadSubscriber.awaitTerminalEvent();
        payloadSubscriber.assertComplete();
        payloadSubscriber.assertValueCount(1);

        // assert that handleRequestSubscription has a complete event
        payloadSubscriber = new TestSubscriber<>();
        handler.handleSubscription(payload).subscribe(payloadSubscriber);
        payloadSubscriber.awaitTerminalEvent();
        payloadSubscriber.assertComplete();
        payloadSubscriber.assertValueCount(1);

    }
}
