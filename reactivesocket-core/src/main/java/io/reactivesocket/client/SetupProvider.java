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

import io.reactivesocket.client.ReactiveSocketClient.SocketAcceptor;
import io.reactivesocket.lease.DefaultLeaseHonoringSocket;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.Frame.Setup;
import io.reactivesocket.lease.LeaseHonoringSocket;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.frame.SetupFrameFlyweight;
import io.reactivesocket.util.PayloadImpl;
import org.reactivestreams.Publisher;

import java.util.function.Function;

/**
 * A provider for ReactiveSocket setup from a client.
 */
public interface SetupProvider {

    int DEFAULT_FLAGS = SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE | SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;
    int DEFAULT_MAX_KEEP_ALIVE_MISSING_ACK = 3;
    String DEFAULT_METADATA_MIME_TYPE = "application/x.reactivesocket.meta+cbor";
    String DEFAULT_DATA_MIME_TYPE = "application/binary";

    Publisher<ReactiveSocket> accept(DuplexConnection connection, SocketAcceptor acceptor);

    SetupProvider dataMimeType(String dataMimeType);

    SetupProvider metadataMimeType(String metadataMimeType);

    SetupProvider honorLease(Function<ReactiveSocket, LeaseHonoringSocket> leaseDecorator);

    SetupProvider disableLease();

    static SetupProvider keepAlive(KeepAliveProvider keepAliveProvider) {
        int period = keepAliveProvider.getKeepAlivePeriodMillis();
        Frame setupFrame =
                Setup.from(DEFAULT_FLAGS, period, keepAliveProvider.getMissedKeepAliveThreshold() * period,
                           DEFAULT_METADATA_MIME_TYPE, DEFAULT_DATA_MIME_TYPE, PayloadImpl.EMPTY);
        return new SetupProviderImpl(setupFrame, reactiveSocket -> new DefaultLeaseHonoringSocket(reactiveSocket),
                                     keepAliveProvider, Throwable::printStackTrace);
    }
}
