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

package io.reactivesocket.client;

import io.reactivesocket.Payload;
import io.reactivesocket.client.ReactiveSocketClient.SocketAcceptor;
import io.reactivesocket.events.ClientEventListener;
import io.reactivesocket.events.EventSource;
import io.reactivesocket.lease.DefaultLeaseHonoringSocket;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.Frame.Setup;
import io.reactivesocket.lease.DisableLeaseSocket;
import io.reactivesocket.lease.LeaseHonoringSocket;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.frame.SetupFrameFlyweight;
import io.reactivesocket.util.PayloadImpl;
import org.reactivestreams.Publisher;

import java.util.function.Function;

/**
 * A provider for ReactiveSocket setup from a client.
 */
public interface SetupProvider extends EventSource<ClientEventListener> {

    int DEFAULT_FLAGS = SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE | SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;
    int DEFAULT_MAX_KEEP_ALIVE_MISSING_ACK = 3;
    String DEFAULT_METADATA_MIME_TYPE = "application/x.reactivesocket.meta+cbor";
    String DEFAULT_DATA_MIME_TYPE = "application/binary";

    /**
     * Accept a {@link DuplexConnection} and does the setup to produce a {@code ReactiveSocket}.
     *
     * @param connection To setup.
     * @param acceptor of the newly created {@code ReactiveSocket}.
     *
     * @return Asynchronous source for the created {@code ReactiveSocket}
     */
    Publisher<ReactiveSocket> accept(DuplexConnection connection, SocketAcceptor acceptor);

    /**
     * Creates a new {@code SetupProvider} by modifying the mime type for data payload of this {@code SetupProvider}
     *
     * @param dataMimeType Mime type for data payloads for all created {@code ReactiveSocket}
     *
     * @return A new {@code SetupProvider} instance.
     */
    SetupProvider dataMimeType(String dataMimeType);

    /**
     * Creates a new {@code SetupProvider} by modifying the mime type for metadata payload of this {@code SetupProvider}
     *
     * @param metadataMimeType Mime type for metadata payloads for all created {@code ReactiveSocket}
     *
     * @return A new {@code SetupProvider} instance.
     */
    SetupProvider metadataMimeType(String metadataMimeType);

    /**
     * Creates a new {@code SetupProvider} that honors leases sent from the server.
     *
     * @param leaseDecorator A factory that decorates a {@code ReactiveSocket} to honor leases.
     *
     * @return A new {@code SetupProvider} instance.
     */
    SetupProvider honorLease(Function<ReactiveSocket, LeaseHonoringSocket> leaseDecorator);

    /**
     * Creates a new {@code SetupProvider} that does not honor leases.
     *
     * @return A new {@code SetupProvider} instance.
     */
    SetupProvider disableLease();

    /**
     * Creates a new {@code SetupProvider} that does not honor leases.
     *
     * @param socketFactory A factory to create {@link DisableLeaseSocket} for each accepted socket.
     *
     * @return A new {@code SetupProvider} instance.
     */
    SetupProvider disableLease(Function<ReactiveSocket, DisableLeaseSocket> socketFactory);

    /**
     * Creates a new {@code SetupProvider} that uses the passed {@code setupPayload} as the payload for the setup frame.
     * Default instances, do not have any payload.
     *
     * @return A new {@code SetupProvider} instance.
     */
    SetupProvider setupPayload(Payload setupPayload);

    /**
     * Creates a new {@link SetupProvider} using the passed {@code keepAliveProvider} for keep alives to be sent on the
     * {@link ReactiveSocket}s created by this provider.
     *
     * @param keepAliveProvider Provider for keep-alive.
     *
     * @return A new {@code SetupProvider}.
     */
    static SetupProvider keepAlive(KeepAliveProvider keepAliveProvider) {
        int period = keepAliveProvider.getKeepAlivePeriodMillis();
        Frame setupFrame =
                Setup.from(DEFAULT_FLAGS, period, keepAliveProvider.getMissedKeepAliveThreshold() * period,
                           DEFAULT_METADATA_MIME_TYPE, DEFAULT_DATA_MIME_TYPE, PayloadImpl.EMPTY);
        return new SetupProviderImpl(setupFrame, reactiveSocket -> new DefaultLeaseHonoringSocket(reactiveSocket),
                                     keepAliveProvider, Throwable::printStackTrace);
    }
}
