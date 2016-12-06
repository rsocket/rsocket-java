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

package io.reactivesocket.test.util;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Frame;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.lease.DefaultLeaseEnforcingSocket;
import io.reactivesocket.lease.DefaultLeaseEnforcingSocket.LeaseDistributor;
import io.reactivesocket.lease.DisableLeaseSocket;
import io.reactivesocket.lease.Lease;
import io.reactivesocket.lease.LeaseImpl;
import io.reactivesocket.local.LocalSendReceiveTest.LocalRule;
import io.reactivesocket.reactivestreams.extensions.internal.CancellableImpl;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.TransportServer.StartedServer;
import io.reactivex.Single;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static io.reactivesocket.client.KeepAliveProvider.never;
import static io.reactivesocket.client.SetupProvider.keepAlive;
import static org.mockito.Matchers.any;

public class LocalRSRule extends LocalRule {

    private ReactiveSocketServer socketServer;
    private ReactiveSocketClient socketClient;
    private LeaseDistributor leaseDistributorMock;
    private List<Lease> leases;
    private List<Boolean> acceptingSocketCloses;
    private StartedServer started;

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                leases = new CopyOnWriteArrayList<>();
                acceptingSocketCloses = new CopyOnWriteArrayList<>();
                leaseDistributorMock = Mockito.mock(LeaseDistributor.class);
                Mockito.when(leaseDistributorMock.registerSocket(any())).thenReturn(new CancellableImpl());
                init();
                socketServer = ReactiveSocketServer.create(localServer);
                started = socketServer.start((setup, sendingSocket) -> {
                    AbstractReactiveSocket accept = new AbstractReactiveSocket() {
                    };
                    accept.onClose().subscribe(Subscribers.doOnTerminate(() -> acceptingSocketCloses.add(true)));
                    return new DefaultLeaseEnforcingSocket(accept, leaseDistributorMock);
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

    public List<Lease> getLeases() {
        return leases;
    }

    public StartedServer getStartedServer() {
        return started;
    }

    public ReactiveSocketServer getSocketServer() {
        return socketServer;
    }

    public ReactiveSocketClient getSocketClient() {
        return socketClient;
    }

    public List<Boolean> getAcceptingSocketCloses() {
        return acceptingSocketCloses;
    }
}
