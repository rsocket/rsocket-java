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
import io.reactivex.functions.Predicate;
import io.reactivex.processors.UnicastProcessor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import io.reactivex.subscribers.TestSubscriber;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class RemoteSenderTest {

    @Rule
    public final SenderRule rule = new SenderRule();

    @Test
    public void testOnNext() throws Exception {
        final TestSubscriber<Frame> receiverSub = rule.subscribeToSender();
        rule.sender.acceptRequestNFrame(Frame.RequestN.from(rule.streamId, 1));
        rule.sendFrame(FrameType.NEXT);

        receiverSub.assertValueCount(1);
        receiverSub.assertValue(new Predicate<Frame>() {
            @Override
            public boolean test(Frame frame) throws Exception {
                return frame.getType() == FrameType.NEXT;
            }
        });
        assertThat("Unexpected sender cleaned up.", rule.senderCleanedUp, is(false));
    }

    @Test
    public void testOnError() throws Exception {
        final TestSubscriber<Frame> receiverSub = rule.subscribeToSender();
        rule.sender.onError(new NullPointerException("deliberate test exception."));

        receiverSub.assertValueCount(1);
        receiverSub.assertValue(new Predicate<Frame>() {
            @Override
            public boolean test(Frame frame) throws Exception {
                return frame.getType() == FrameType.ERROR;
            }
        });
        receiverSub.assertError(NullPointerException.class);
        receiverSub.assertNotComplete();
        assertThat("Unexpected sender cleaned up.", rule.senderCleanedUp, is(true));
    }

    @Test
    public void testOnComplete() throws Exception {
        final TestSubscriber<Frame> receiverSub = rule.subscribeToSender();
        rule.sender.onComplete();

        receiverSub.assertValueCount(1);
        receiverSub.assertValue(new Predicate<Frame>() {
            @Override
            public boolean test(Frame frame) throws Exception {
                return frame.getType() == FrameType.COMPLETE;
            }
        });

        receiverSub.assertNoErrors();
        receiverSub.assertComplete();
        assertThat("Unexpected sender cleaned up.", rule.senderCleanedUp, is(true));
    }

    @Test
    public void testTransportCancel() throws Exception {
        final TestSubscriber<Frame> receiverSub = rule.subscribeToSender(2);
        rule.sender.acceptRequestNFrame(Frame.RequestN.from(rule.streamId, 2));
        rule.sendFrame(FrameType.NEXT);

        receiverSub.assertValueCount(1);
        receiverSub.assertValue(new Predicate<Frame>() {
            @Override
            public boolean test(Frame frame) throws Exception {
                return frame.getType() == FrameType.NEXT;
            }
        });
        receiverSub.cancel();// Transport cancel.
        assertThat("Sender not cleaned up.", rule.senderCleanedUp, is(true));

        rule.sendFrame(FrameType.NEXT);
        receiverSub.assertValueCount(1);
    }

    @Test
    public void testRemoteCancel() throws Exception {
        final TestSubscriber<Frame> receiverSub = rule.subscribeToSender(2);
        rule.sender.acceptRequestNFrame(Frame.RequestN.from(rule.streamId, 2));
        rule.sendFrame(FrameType.NEXT);

        receiverSub.assertValueCount(1);
        receiverSub.assertValue(new Predicate<Frame>() {
            @Override
            public boolean test(Frame frame) throws Exception {
                return frame.getType() == FrameType.NEXT;
            }
        });
        rule.sender.acceptCancelFrame(Frame.Cancel.from(rule.streamId));// Remote cancel.
        assertThat("Sender not cleaned up.", rule.senderCleanedUp, is(true));

        rule.sendFrame(FrameType.NEXT);
        receiverSub.assertValueCount(1);
    }

    @Test
    public void testOnCompleteWithBuffer() throws Exception {
        final TestSubscriber<Frame> receiverSub = rule.subscribeToSender(0);
        rule.sender.onComplete(); // buffer this terminal event.

        receiverSub.assertNotTerminated();
        receiverSub.request(1); // Now get completion

        receiverSub.assertValueCount(1);
        receiverSub.assertValue(new Predicate<Frame>() {
            @Override
            public boolean test(Frame frame) throws Exception {
                return frame.getType() == FrameType.COMPLETE;
            }
        });
        receiverSub.assertNoErrors();
        receiverSub.assertComplete();
        assertThat("Unexpected sender cleaned up.", rule.senderCleanedUp, is(true));
    }

    @Test
    public void testOnErrorWithBuffer() throws Exception {
        final TestSubscriber<Frame> receiverSub = rule.subscribeToSender(0);
        rule.sender.onError(new NullPointerException("deliberate test exception.")); // buffer this terminal event.

        receiverSub.assertNotTerminated();
        receiverSub.request(1); // Now get completion

        receiverSub.assertValueCount(1);
        receiverSub.assertValue(new Predicate<Frame>() {
            @Override
            public boolean test(Frame frame) throws Exception {
                return frame.getType() == FrameType.ERROR;
            }
        });
        receiverSub.assertError(NullPointerException.class);
        receiverSub.assertNotComplete();
        assertThat("Unexpected sender cleaned up.", rule.senderCleanedUp, is(true));
    }

    @Test
    public void testTransportRequestedMore() throws Exception {
        final TestSubscriber<Frame> receiverSub = rule.subscribeToSender(2);
        rule.sender.request(1); // Remote requested 1
        rule.sendFrame(FrameType.NEXT);
        rule.sendFrame(FrameType.NEXT); // Second onNext gets buffered.

        receiverSub.assertValueCount(1).assertNotTerminated();
        rule.sender.request(1); // Remote requested 1 to now emit second.
        receiverSub.assertValueCount(2).assertNotTerminated();

        receiverSub.request(1); // Transport: 1, remote: 0
        rule.sendFrame(FrameType.NEXT);
        receiverSub.assertValueCount(2).assertNotTerminated();

        rule.sender.request(1); // Remote: 1
        receiverSub.assertValueCount(3).assertNotTerminated();

        receiverSub.request(1); // Transport: 1 to get terminal event.
        rule.source.onComplete();
        receiverSub.assertComplete().assertNoErrors();
    }

    @Test
    public void testRemoteRequestedMore() throws Exception {
        final TestSubscriber<Frame> receiverSub = rule.subscribeToSender(0);
        rule.sender.request(1); // Remote: 1, transport: 0
        rule.sendFrame(FrameType.NEXT); // buffer, transport not ready.

        receiverSub.assertNoValues().assertNotTerminated();
        receiverSub.request(1); // Remote: 1, transport: 1, emit

        receiverSub.assertValueCount(1).assertNotTerminated();
    }

    public static class SenderRule extends ExternalResource {

        private UnicastProcessor<Frame> source;
        private RemoteSender sender;
        private boolean senderCleanedUp;
        private int streamId;

        @Override
        public Statement apply(final Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    source = UnicastProcessor.create();
                    streamId = 10;
                    sender = new RemoteSender(source, () -> senderCleanedUp = true, streamId);
                    base.evaluate();
                }
            };
        }

        public Frame newFrame(FrameType frameType) {
            return Frame.Response.from(streamId, frameType);
        }

        public TestSubscriber<Frame> subscribeToSender(int initialRequestN) {
            final TestSubscriber<Frame> senderSub = TestSubscriber.create(initialRequestN);
            sender.subscribe(senderSub);

            senderSub.assertNotTerminated();
            return senderSub;
        }

        public TestSubscriber<Frame> subscribeToSender() {
            return subscribeToSender(1);
        }

        public void sendFrame(FrameType frameType) {
            source.onNext(newFrame(frameType));
        }

        public void sendFrame(Frame frame) {
            source.onNext(frame);
        }
    }
}