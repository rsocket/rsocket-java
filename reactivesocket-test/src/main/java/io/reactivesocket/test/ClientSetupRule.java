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

package io.reactivesocket.test;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketConnector;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.functions.Func0;
import rx.observers.TestSubscriber;

import java.net.SocketAddress;
import java.util.function.Function;

import static io.reactivesocket.test.TestUtil.*;
import static rx.RxReactiveStreams.*;

public class ClientSetupRule extends ExternalResource {

    private final ReactiveSocketConnector<SocketAddress> client;
    private final Func0<SocketAddress> serverStarter;
    private SocketAddress serverAddress;
    private ReactiveSocket reactiveSocket;

    public ClientSetupRule(ReactiveSocketConnector<SocketAddress> connector, Func0<SocketAddress> serverStarter) {
        client = connector;
        this.serverStarter = serverStarter;
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                serverAddress = serverStarter.call();
                reactiveSocket = toObservable(client.connect(serverAddress)).toSingle().toBlocking().value();

                base.evaluate();
            }
        };
    }

    public ReactiveSocketConnector<SocketAddress> getClient() {
        return client;
    }

    public SocketAddress getServerAddress() {
        return serverAddress;
    }

    public ReactiveSocket getReactiveSocket() {
        return reactiveSocket;
    }

    public void testRequestResponseN(int count) {
        TestSubscriber<String> ts = TestSubscriber.create();
        Observable
                .range(1, count)
                .flatMap(i -> toObservable(getReactiveSocket().requestResponse(utf8EncodedPayload("hello", "metadata")))
                                         .map(payload -> byteToString(payload.getData()))
                )
                .doOnError(Throwable::printStackTrace)
                .subscribe(ts);

        ts.awaitTerminalEvent();
        ts.assertValueCount(count);
        ts.assertNoErrors();
        ts.assertCompleted();
    }

    public void testRequestSubscription() {
        _testStream(
                socket -> toPublisher(toObservable(socket.requestSubscription(utf8EncodedPayload("hello", "metadata")))
                                              .take(10)));
    }

    public void testRequestStream() {
        _testStream(socket -> socket.requestStream(utf8EncodedPayload("hello", "metadata")));
    }

    private void _testStream(Function<ReactiveSocket, Publisher<Payload>> invoker) {
        TestSubscriber<Payload> ts = TestSubscriber.create();
        Publisher<Payload> publisher = invoker.apply(reactiveSocket);
        toObservable(publisher).subscribe(ts);
        ts.awaitTerminalEvent();
        ts.assertValueCount(10);
        ts.assertNoErrors();
        ts.assertCompleted();
    }
}
