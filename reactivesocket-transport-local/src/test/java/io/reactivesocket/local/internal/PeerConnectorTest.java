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

package io.reactivesocket.local.internal;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.Frame.RequestN;
import io.reactivex.Flowable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import io.reactivex.subscribers.TestSubscriber;

import java.nio.channels.ClosedChannelException;

import static org.junit.Assert.fail;

public class PeerConnectorTest {

    @Rule
    public final ConnectorRule rule = new ConnectorRule();

    @Test(timeout = 10000)
    public void testSendToServer() throws Exception {
        Frame frame = rule.sendToServer();
        rule.serverRecv.assertNotTerminated().assertValues(frame);
    }

    @Test(timeout = 10000)
    public void testSendToClient() throws Exception {
        Frame frame = rule.sendToClient();
        rule.clientRecv.assertNotTerminated().assertValues(frame);
    }

    @Test(timeout = 10000)
    public void testServerClose() throws Exception {
        testClose(rule.connector.forServer(), rule.connector.forClient());
    }

    @Test(timeout = 10000)
    public void testClientClose() throws Exception {
        testClose(rule.connector.forClient(), rule.connector.forServer());
    }

    @Test(timeout = 10000)
    public void testDuplicateClientReceiveSubscription() throws Exception {
        testDuplicateReceiveSubscription(rule.connector.forClient());
    }

    @Test(timeout = 10000)
    public void testDuplicateServerReceiveSubscription() throws Exception {
        testDuplicateReceiveSubscription(rule.connector.forServer());
    }

    @Test(timeout = 10000)
    public void testReceiveErrorOnClose() throws Exception {
        testClose(rule.connector.forClient(), rule.connector.forServer());
        rule.clientRecv.assertError(ClosedChannelException.class);
        rule.serverRecv.assertError(ClosedChannelException.class);
    }

    protected void testDuplicateReceiveSubscription(DuplexConnection connection) {
        TestSubscriber<Frame> subscriber = TestSubscriber.create();
        connection.receive().subscribe(subscriber); // Rule subscribes first
        subscriber.assertError(IllegalStateException.class);
    }

    private static void testClose(DuplexConnection first, DuplexConnection second) {
        TestSubscriber<Void> closeSub = TestSubscriber.create();
        first.close().subscribe(closeSub);
        await(closeSub);

        TestSubscriber<Void> peerCloseSub = TestSubscriber.create();
        second.onClose().subscribe(peerCloseSub);
        peerCloseSub.assertComplete().assertNoErrors();
    }

    private static void await(TestSubscriber<Void> closeSub){
        try {
            closeSub.await();
        } catch (InterruptedException e) {
            fail("Interrupted while waiting for completion.");
        }
    }

    public static class ConnectorRule extends ExternalResource {

        private PeerConnector connector;
        private TestSubscriber<Frame> clientRecv;
        private TestSubscriber<Frame> serverRecv;

        @Override
        public Statement apply(final Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    connector = PeerConnector.connect("test", 1);
                    serverRecv = TestSubscriber.create();
                    clientRecv = TestSubscriber.create();
                    connector.forServer().receive().subscribe(serverRecv);
                    connector.forClient().receive().subscribe(clientRecv);
                    base.evaluate();
                }
            };
        }

        public Frame sendToServer() {
            return sendTo(connector.forClient());
        }

        public Frame sendToClient() {
            return sendTo(connector.forServer());
        }

        protected Frame sendTo(DuplexConnection target) {
            Frame frame = RequestN.from(1, 1);
            TestSubscriber<Void> sendSub = TestSubscriber.create();
            Flowable.fromPublisher(target.sendOne(frame)).subscribe(sendSub);
            await(sendSub);
            return frame;
        }
    }
}