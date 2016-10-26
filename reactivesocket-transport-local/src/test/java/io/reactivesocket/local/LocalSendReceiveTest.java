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

package io.reactivesocket.local;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.Frame.RequestN;
import io.reactivex.Single;
import org.junit.Rule;
import org.junit.Test;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.concurrent.ThreadLocalRandom;

public class LocalSendReceiveTest {

    @Rule
    public final LocalRule rule = new LocalRule();

    @Test(timeout = 10000)
    public void testSendReceive() throws Exception {
        DuplexConnection connection = rule.connect();
        TestSubscriber<Frame> receiveSub = TestSubscriber.create();
        connection.receive().subscribe(receiveSub);
        Frame frame = RequestN.from(1, 1);
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        connection.sendOne(frame).subscribe(subscriber);
        subscriber.await().assertNoErrors();

        receiveSub.assertNoErrors().assertNotComplete().assertValues(frame);
    }

    @Test(timeout = 10000)
    public void testClose() throws Exception {
        DuplexConnection connection = rule.connect();

        TestSubscriber<Frame> receiveSub = TestSubscriber.create();
        connection.receive().subscribe(receiveSub);
        Frame frame = RequestN.from(1, 1);
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        connection.sendOne(frame).subscribe(subscriber);
        subscriber.await().assertNoErrors();

        receiveSub.assertNoErrors().assertNotComplete().assertValues(frame);
        TestSubscriber<Void> closeSub = TestSubscriber.create();
        connection.close().subscribe(closeSub);
        closeSub.awaitTerminalEvent();
        closeSub.assertNoErrors();
    }

    public static class LocalRule extends ExternalResource {

        private LocalClient localClient;
        private String name;

        @Override
        public Statement apply(final Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    name = "test-send-receive-server-" + ThreadLocalRandom.current().nextInt();
                    LocalServer.create(name)
                               .start(duplexConnection -> {
                                   return duplexConnection.send(duplexConnection.receive());
                               });

                    localClient = LocalClient.create(name);
                    base.evaluate();
                }
            };
        }

        public DuplexConnection connect() {
            return Single.fromPublisher(localClient.connect()).blockingGet();
        }
    }
}
