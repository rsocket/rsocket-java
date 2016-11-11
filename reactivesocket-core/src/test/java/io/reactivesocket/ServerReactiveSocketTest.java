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

package io.reactivesocket;

import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.test.util.TestDuplexConnection;
import io.reactivesocket.util.PayloadImpl;
import org.junit.Rule;
import org.junit.Test;
import org.reactivestreams.Publisher;
import io.reactivex.subscribers.TestSubscriber;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ServerReactiveSocketTest {

    @Rule
    public final ServerSocketRule rule = new ServerSocketRule();

    @Test(timeout = 2000)
    public void testHandleKeepAlive() throws Exception {
        rule.connection.addToReceivedBuffer(Frame.Keepalive.from(Frame.NULL_BYTEBUFFER, true));
        Frame sent = rule.connection.awaitSend();
        assertThat("Unexpected frame sent.", sent.getType(), is(FrameType.KEEPALIVE));
        /*Keep alive ack must not have respond flag else, it will result in infinite ping-pong of keep alive frames.*/
        assertThat("Unexpected keep-alive frame respond flag.", Frame.Keepalive.hasRespondFlag(sent), is(false));
    }


    @Test(timeout = 2000)
    public void testHandleResponseFrameNoError() throws Exception {
        final int streamId = 4;
        rule.connection.clearSendReceiveBuffers();

        rule.sendRequest(streamId, FrameType.REQUEST_RESPONSE);

        Collection<TestSubscriber<Frame>> sendSubscribers = rule.connection.getSendSubscribers();
        assertThat("Request not sent.", sendSubscribers, hasSize(1));
        assertThat("Unexpected error.", rule.errors, is(empty()));
        TestSubscriber<Frame> sendSub = sendSubscribers.iterator().next();
        sendSub.request(2);
        assertThat("Unexpected frame sent.", rule.connection.awaitSend().getType(), is(FrameType.COMPLETE));
    }

    @Test(timeout = 2000)
    public void testHandlerEmitsError() throws Exception {
        final int streamId = 4;
        rule.sendRequest(streamId, FrameType.REQUEST_STREAM);
        assertThat("Unexpected error.", rule.errors, is(empty()));
        assertThat("Unexpected frame sent.", rule.connection.awaitSend().getType(), is(FrameType.ERROR));
    }

    @Test(timeout = 2_0000)
    public void testCancel() throws Exception {
        final int streamId = 4;
        final AtomicBoolean cancelled = new AtomicBoolean();
        rule.setAcceptingSocket(new AbstractReactiveSocket() {
            @Override
            public Publisher<Payload> requestResponse(Payload payload) {
                return Px.<Payload>never()
                         .doOnCancel(() -> cancelled.set(true));
            }
        });
        rule.sendRequest(streamId, FrameType.REQUEST_RESPONSE);

        assertThat("Unexpected error.", rule.errors, is(empty()));
        assertThat("Unexpected frame sent.", rule.connection.getSent(), is(empty()));

        rule.connection.addToReceivedBuffer(Frame.Cancel.from(streamId));
        assertThat("Unexpected frame sent.", rule.connection.getSent(), is(empty()));
        assertThat("Subscription not cancelled.", cancelled.get(), is(true));
    }

    public static class ServerSocketRule extends AbstractSocketRule<ServerReactiveSocket> {

        private ReactiveSocket acceptingSocket;

        @Override
        protected void init() {
            acceptingSocket = new AbstractReactiveSocket() {
                @Override
                public Publisher<Payload> requestResponse(Payload payload) {
                    return Px.just(payload);
                }
            };
            super.init();
            socket.start();
        }

        public void setAcceptingSocket(ReactiveSocket acceptingSocket) {
            this.acceptingSocket = acceptingSocket;
            connection = new TestDuplexConnection();
            connectSub = TestSubscriber.create();
            errors = new ConcurrentLinkedQueue<>();
            super.init();
            socket.start();
        }

        @Override
        protected ServerReactiveSocket newReactiveSocket() {
            return new ServerReactiveSocket(connection, acceptingSocket, throwable -> errors.add(throwable));
        }

        private void sendRequest(int streamId, FrameType frameType) {
            Frame request = Frame.Request.from(streamId, frameType, PayloadImpl.EMPTY, 1);
            connection.addToReceivedBuffer(request);
            connection.addToReceivedBuffer(Frame.RequestN.from(streamId, 2));
        }
    }
}