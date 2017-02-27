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

import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.exceptions.ApplicationException;
import io.reactivesocket.exceptions.RejectedSetupException;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Flowable;
import io.reactivex.processors.PublishProcessor;
import org.junit.Rule;
import org.junit.Test;
import org.reactivestreams.Publisher;
import io.reactivex.subscribers.TestSubscriber;

import java.util.ArrayList;
import java.util.List;

import static io.reactivesocket.FrameType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ClientReactiveSocketTest {

    @Rule
    public final ClientSocketRule rule = new ClientSocketRule();

    @Test(timeout = 2_000)
    public void testKeepAlive() throws Exception {
        rule.keepAliveTicks.onNext(1L);
        assertThat("Unexpected frame sent.", rule.connection.awaitSend().getType(), is(KEEPALIVE));
    }

    @Test(timeout = 2_000)
    public void testInvalidFrameOnStream0() throws Throwable {
        rule.connection.addToReceivedBuffer(Frame.RequestN.from(0, 10));
        assertThat("Unexpected errors.", rule.errors, hasSize(1));
        assertThat("Unexpected error received.", rule.errors, contains(instanceOf(IllegalStateException.class)));
    }

    @Test(timeout = 2_000)
    public void testHandleSetupException() throws Throwable {
        rule.connection.addToReceivedBuffer(Frame.Error.from(0, new RejectedSetupException("boom")));
        assertThat("Unexpected errors.", rule.errors, hasSize(1));
        assertThat("Unexpected error received.", rule.errors, contains(instanceOf(RejectedSetupException.class)));
    }

    @Test(timeout = 2_000)
    public void testHandleApplicationException() throws Throwable {
        rule.connection.clearSendReceiveBuffers();
        Publisher<Payload> response = rule.socket.requestResponse(PayloadImpl.EMPTY);
        TestSubscriber<Payload> responseSub = TestSubscriber.create();
        response.subscribe(responseSub);

        int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
        rule.connection.addToReceivedBuffer(Frame.Error.from(streamId, new ApplicationException(PayloadImpl.EMPTY)));

        responseSub.assertError(ApplicationException.class);
    }

    @Test(timeout = 2_000)
    public void testHandleValidFrame() throws Throwable {
        Publisher<Payload> response = rule.socket.requestResponse(PayloadImpl.EMPTY);
        TestSubscriber<Payload> responseSub = TestSubscriber.create();
        response.subscribe(responseSub);

        int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
        rule.connection.addToReceivedBuffer(Frame.Response.from(streamId, NEXT_COMPLETE, PayloadImpl.EMPTY));

        responseSub.assertValueCount(1);
        responseSub.assertComplete();
    }

    @Test
    public void testRequestReplyWithCancel() throws Throwable {
        rule.connection.clearSendReceiveBuffers(); // clear setup frame
        Publisher<Payload> response = rule.socket.requestResponse(PayloadImpl.EMPTY);
        TestSubscriber<Payload> responseSub = TestSubscriber.create(0);
        response.subscribe(responseSub);
        responseSub.cancel();

        responseSub.assertValueCount(0);
        responseSub.assertNotTerminated();

        assertThat("Unexpected frame sent on the connection.", rule.connection.awaitSend().getType(), is(REQUEST_RESPONSE));
        assertThat("Unexpected frame sent on the connection.", rule.connection.awaitSend().getType(), is(CANCEL));
    }

    @Test(timeout = 2_000)
    public void testRequestReplyErrorOnSend() throws Throwable {
        rule.connection.setAvailability(0); // Fails send
        Publisher<Payload> response = rule.socket.requestResponse(PayloadImpl.EMPTY);
        TestSubscriber<Payload> responseSub = TestSubscriber.create();
        Flowable.fromPublisher(response)
                .subscribe(responseSub);

        responseSub.assertError(RuntimeException.class);
    }

    @Test
    public void testLazyRequestResponse() throws Exception {
        Publisher<Payload> response = rule.socket.requestResponse(PayloadImpl.EMPTY);
        int streamId = sendRequestResponse(response);
        rule.connection.clearSendReceiveBuffers();
        int streamId2 = sendRequestResponse(response);
        assertThat("Stream ID reused.", streamId2, not(equalTo(streamId)));
    }

    public int sendRequestResponse(Publisher<Payload> response) {
        TestSubscriber<Payload> sub = TestSubscriber.create();
        response.subscribe(sub);
        int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
        rule.connection.addToReceivedBuffer(Frame.Response.from(streamId, RESPONSE, PayloadImpl.EMPTY));
        sub.assertValueCount(1).assertNoErrors();
        return streamId;
    }

    public static class ClientSocketRule extends AbstractSocketRule<ClientReactiveSocket> {

        private final PublishProcessor<Long> keepAliveTicks = PublishProcessor.create();

        @Override
        protected ClientReactiveSocket newReactiveSocket() {
            return new ClientReactiveSocket(connection,
                                            throwable -> errors.add(throwable), StreamIdSupplier.clientSupplier(),
                                            KeepAliveProvider.from(1, keepAliveTicks)).start(lease -> {});
        }

        public int getStreamIdForRequestType(FrameType expectedFrameType) {
            assertThat("Unexpected frames sent.", connection.getSent(), hasSize(greaterThanOrEqualTo(1)));
            List<FrameType> framesFound = new ArrayList<>();
            for (Frame frame : connection.getSent()) {
                if (frame.getType() == expectedFrameType) {
                    return frame.getStreamId();
                }
                framesFound.add(frame.getType());
            }
            throw new AssertionError("No frames sent with frame type: " + expectedFrameType + ", frames found: "
                                     + framesFound);
        }
    }

}