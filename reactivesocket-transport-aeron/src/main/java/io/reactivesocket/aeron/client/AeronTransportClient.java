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

package io.reactivesocket.aeron.client;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.aeron.AeronDuplexConnection;
import io.reactivesocket.aeron.internal.reactivestreams.AeronChannel;
import io.reactivesocket.aeron.internal.reactivestreams.AeronClientChannelConnector;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.transport.TransportClient;
import org.reactivestreams.Publisher;

import java.util.Objects;

/**
 * {@link TransportClient} implementation that uses Aeron as a transport
 */
public class AeronTransportClient implements TransportClient {
    private final AeronClientChannelConnector connector;
    private final AeronClientChannelConnector.AeronClientConfig config;

    public AeronTransportClient(AeronClientChannelConnector connector, AeronClientChannelConnector.AeronClientConfig config) {
        Objects.requireNonNull(config);
        Objects.requireNonNull(connector);
        this.connector = connector;
        this.config = config;
    }

    @Override
    public Publisher<DuplexConnection> connect() {
        Publisher<AeronChannel> channelPublisher = connector.apply(config);

        return Px
            .from(channelPublisher)
            .map(aeronChannel -> new AeronDuplexConnection("client", aeronChannel));
    }
}
