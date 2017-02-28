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

package io.reactivesocket.internal;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.exceptions.ApplicationException;
import io.reactivesocket.exceptions.CancelException;
import io.reactivesocket.test.util.TestDuplexConnection;
import io.reactivesocket.util.PayloadImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import io.reactivex.subscribers.TestSubscriber;
import reactor.core.publisher.UnicastProcessor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class RemoteReceiverTest {

    @Rule
    public final ReceiverRule rule = new ReceiverRule();

    @Test
    public void testCompleteFrame() throws Exception {
        final TestSubscriber<Payload> receiverSub = rule.subscribeToReceiver();
        rule.assertRequestNSent(1);
        rule.sendFrame(FrameType.COMPLETE);

        receiverSub.assertComplete();
        assertThat("Receiver not cleaned up.", rule.receiverCleanedUp, is(true));
    }

    @Test
    public void testErrorFrame() throws Exception {
        final TestSubscriber<Payload> receiverSub = rule.subscribeToReceiver();
        rule.assertRequestNSent(1);
        rule.sendFrame(Frame.Error.from(rule.streamId, new ApplicationException(PayloadImpl.EMPTY)));

        receiverSub.assertNotComplete();
        receiverSub.assertError(ApplicationException.class);
        assertThat("Receiver not cleaned up.", rule.receiverCleanedUp, is(true));
    }

    @Test
    public void testNextFrame() throws Exception {
        final TestSubscriber<Payload> receiverSub = rule.subscribeToReceiver();
        rule.assertRequestNSent(1);
        rule.sendFrame(FrameType.NEXT);

        receiverSub.assertValueCount(1);
        receiverSub.assertNotTerminated();
        assertThat("Receiver cleaned up.", rule.receiverCleanedUp, is(false));
    }

    @Test
    public void testNextCompleteFrame() throws Exception {
        final TestSubscriber<Payload> receiverSub = rule.subscribeToReceiver();
        rule.assertRequestNSent(1);
        rule.sendFrame(FrameType.NEXT_COMPLETE);

        receiverSub.assertValueCount(1);
        receiverSub.assertComplete().assertNoErrors();
        assertThat("Receiver not cleaned up.", rule.receiverCleanedUp, is(true));
    }

    @Test
    public void testCancel() throws Exception {
        final TestSubscriber<Payload> receiverSub = rule.subscribeToReceiver();
        rule.assertRequestNSent(1);
        rule.connection.clearSendReceiveBuffers();
        rule.receiver.cancel();

        receiverSub.assertNoValues().assertError(CancelException.class);
        assertThat("Receiver not cleaned up.", rule.receiverCleanedUp, is(true));

        rule.source.onNext(rule.newFrame(FrameType.NEXT));
        receiverSub.assertNoValues();
    }

    @Test
    public void testRequestNBufferBeforeWriteReady() throws Exception {
        rule.connection.setInitialSendRequestN(0);
        final TestSubscriber<Payload> receiverSub = rule.subscribeToReceiver(0);
        assertThat("Unexpected send subscribers on the connection.", rule.connection.getSendSubscribers(), hasSize(1));
        TestSubscriber<Frame> sendSubscriber = rule.connection.getSendSubscribers().iterator().next();
        assertThat("Unexpected frames sent on the connection.", rule.connection.getSent(), is(empty()));
        receiverSub.request(7);
        receiverSub.request(8);

        sendSubscriber.request(1);// Now request to send requestN frame.
        rule.assertRequestNSent(15); // Cumulate requestN post buffering
    }

    @Test
    public void testCancelBufferBeforeWriteReady() throws Exception {
        rule.connection.setInitialSendRequestN(0);
        final TestSubscriber<Payload> receiverSub = rule.subscribeToReceiver(0);
        assertThat("Unexpected send subscribers on the connection.", rule.connection.getSendSubscribers(), hasSize(1));
        TestSubscriber<Frame> sendSubscriber = rule.connection.getSendSubscribers().iterator().next();
        assertThat("Unexpected frames sent on the connection.", rule.connection.getSent(), is(empty()));
        receiverSub.cancel();

        sendSubscriber.request(1);// Now request to send cancel frame.
        rule.assertCancelSent();
        assertThat("Receiver not cleaned up.", rule.receiverCleanedUp, is(true));
    }

    @Test
    public void testMissedComplete() throws Exception {
        rule.receiver.onComplete();
        final TestSubscriber<Payload> receiverSub = TestSubscriber.create();
        rule.receiver.subscribe(receiverSub);
        receiverSub.assertComplete().assertNoErrors();
    }

    @Test
    public void testMissedError() throws Exception {
        rule.receiver.onError(new NullPointerException("Deliberate exception"));
        final TestSubscriber<Payload> receiverSub = TestSubscriber.create();
        rule.receiver.subscribe(receiverSub);
        receiverSub.assertError(NullPointerException.class).assertNotComplete();
    }

    @Test(expected = IllegalStateException.class)
    public void testOnNextWithoutSubscribe() throws Exception {
        rule.receiver.onNext(Frame.RequestN.from(1, 1));
    }

    public static class ReceiverRule extends ExternalResource {

        private TestDuplexConnection connection;
        private UnicastProcessor<Frame> source;
        private RemoteReceiver receiver;
        private boolean receiverCleanedUp;
        private int streamId;

        @Override
        public Statement apply(final Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    connection = new TestDuplexConnection();
                    streamId = 10;
                    source = UnicastProcessor.create();
                    receiver = new RemoteReceiver(connection, streamId, () -> receiverCleanedUp = true, null, null,
                                                  true);
                    base.evaluate();
                }
            };
        }

        public Frame newFrame(FrameType frameType) {
            return Frame.Response.from(streamId, frameType);
        }

        public TestSubscriber<Payload> subscribeToReceiver(int initialRequestN) {
            final TestSubscriber<Payload> receiverSub = TestSubscriber.create(initialRequestN);
            receiver.subscribe(receiverSub);
            source.subscribe(receiver);

            receiverSub.assertNotTerminated();
            return receiverSub;
        }

        public TestSubscriber<Payload> subscribeToReceiver() {
            return subscribeToReceiver(1);
        }

        public void sendFrame(FrameType frameType) {
            source.onNext(newFrame(frameType));
        }

        public void sendFrame(Frame frame) {
            source.onNext(frame);
        }

        public void assertRequestNSent(int requestN) {
            assertThat("Unexpected frames sent.", connection.getSent(), hasSize(greaterThanOrEqualTo(1)));
            Frame next = connection.getSent().iterator().next();
            assertThat("Unexpected frame type.", next.getType(), is(FrameType.REQUEST_N));
            assertThat("Unexpected requestN sent.", Frame.RequestN.requestN(next), is(requestN));
        }

        public void assertCancelSent() {
            assertThat("Unexpected frames sent.", connection.getSent(), hasSize(greaterThanOrEqualTo(1)));
            Frame next = connection.getSent().iterator().next();
            assertThat("Unexpected frame type.", next.getType(), is(FrameType.CANCEL));
        }
    }
}