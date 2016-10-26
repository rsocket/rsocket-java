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

package io.reactivesocket;

import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.lease.DefaultLeaseEnforcingSocket;
import io.reactivesocket.lease.DefaultLeaseEnforcingSocket.LeaseDistributor;
import io.reactivesocket.lease.DisableLeaseSocket;
import io.reactivesocket.lease.Lease;
import io.reactivesocket.lease.LeaseImpl;
import io.reactivesocket.local.LocalSendReceiveTest.LocalRule;
import io.reactivesocket.reactivestreams.extensions.internal.CancellableImpl;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Single;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static io.reactivesocket.client.KeepAliveProvider.*;
import static io.reactivesocket.client.SetupProvider.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.*;

public class ClientDishonorLeaseTest {

    @Rule
    public final LocalRSRule rule = new LocalRSRule();

    @Test(timeout = 10000)
    public void testNoLeasesSentToClient() throws Exception {
        ReactiveSocket socket = rule.connectSocket();
        rule.sendLease();

        TestSubscriber<Payload> s = TestSubscriber.create();
        socket.requestResponse(PayloadImpl.EMPTY).subscribe(s);
        s.awaitTerminalEvent();

        assertThat("Unexpected leases received by the client.", rule.leases, is(empty()));
    }

    public static class LocalRSRule extends LocalRule {

        private ReactiveSocketServer socketServer;
        private ReactiveSocketClient socketClient;
        private LeaseDistributor leaseDistributorMock;
        private List<Lease> leases;

        @Override
        public Statement apply(final Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    leases = new CopyOnWriteArrayList<>();
                    leaseDistributorMock = Mockito.mock(LeaseDistributor.class);
                    Mockito.when(leaseDistributorMock.registerSocket(any())).thenReturn(new CancellableImpl());
                    init();
                    socketServer = ReactiveSocketServer.create(localServer);
                    socketServer.start((setup, sendingSocket) -> {
                        return new DefaultLeaseEnforcingSocket(new AbstractReactiveSocket() { }, leaseDistributorMock);
                    });
                    socketClient = ReactiveSocketClient.create(localClient, keepAlive(never())
                            .disableLease(reactiveSocket -> new DisableLeaseSocket(reactiveSocket) {
                                @Override
                                public void accept(Lease lease) {
                                    leases.add(lease);
                                }
                            }));
                    base.evaluate();
                }
            };
        }

        public ReactiveSocket connectSocket() {
            return Single.fromPublisher(socketClient.connect()).blockingGet();
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Consumer<Lease> sendLease() {
            ArgumentCaptor<Consumer> leaseConsumerCaptor = ArgumentCaptor.forClass(Consumer.class);
            Mockito.verify(leaseDistributorMock).registerSocket(leaseConsumerCaptor.capture());
            Consumer<Lease> leaseConsumer = leaseConsumerCaptor.getValue();
            leaseConsumer.accept(new LeaseImpl(1, 1, Frame.NULL_BYTEBUFFER));
            return leaseConsumer;
        }
    }
}
