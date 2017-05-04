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

package io.rsocket.client;

import io.rsocket.ClientReactiveSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.Payload;
import io.rsocket.ReactiveSocket;
import io.rsocket.ServerReactiveSocket;
import io.rsocket.StreamIdSupplier;
import io.rsocket.client.ReactiveSocketClient.SocketAcceptor;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.lease.DisableLeaseSocket;
import io.rsocket.lease.LeaseEnforcingSocket;
import io.rsocket.lease.LeaseHonoringSocket;
import io.rsocket.util.PayloadImpl;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

import static io.rsocket.Frame.Setup.from;
import static io.rsocket.Frame.Setup.getFlags;
import static io.rsocket.Frame.Setup.keepaliveInterval;
import static io.rsocket.Frame.Setup.maxLifetime;

final class SetupProviderImpl implements SetupProvider {

    private final Frame setupFrame;
    private final Function<ReactiveSocket, ? extends LeaseHonoringSocket> leaseDecorator;
    private final Consumer<Throwable> errorConsumer;
    private final KeepAliveProvider keepAliveProvider;

    SetupProviderImpl(Frame setupFrame, Function<ReactiveSocket, ? extends LeaseHonoringSocket> leaseDecorator,
                      KeepAliveProvider keepAliveProvider, Consumer<Throwable> errorConsumer) {
        this.keepAliveProvider = keepAliveProvider;
        this.errorConsumer = errorConsumer;
        Frame.ensureFrameType(FrameType.SETUP, setupFrame);
        this.leaseDecorator = leaseDecorator;
        this.setupFrame = setupFrame;
    }

    @Override
    public Mono<ReactiveSocket> accept(DuplexConnection connection, SocketAcceptor acceptor) {
        return connection.sendOne(setupFrame)
            .then(() -> {
                ClientServerInputMultiplexer multiplexer = new ClientServerInputMultiplexer(connection);
                ClientReactiveSocket sendingSocket =
                    new ClientReactiveSocket(multiplexer.asClientConnection(), errorConsumer,
                        StreamIdSupplier.clientSupplier(),
                        keepAliveProvider);
                LeaseHonoringSocket leaseHonoringSocket = leaseDecorator.apply(sendingSocket);

                sendingSocket.start(leaseHonoringSocket);

                LeaseEnforcingSocket acceptingSocket = acceptor.accept(sendingSocket);
                ServerReactiveSocket receivingSocket = new ServerReactiveSocket(multiplexer.asServerConnection(),
                    acceptingSocket, true,
                    errorConsumer);
                receivingSocket.start();

                return Mono.just(leaseHonoringSocket);
            });
    }

    @Override
    public SetupProvider dataMimeType(String dataMimeType) {
        Frame newSetup = from(getFlags(setupFrame), keepaliveInterval(setupFrame), maxLifetime(setupFrame),
            Frame.Setup.metadataMimeType(setupFrame), dataMimeType, new PayloadImpl(setupFrame));
        setupFrame.release();
        return new SetupProviderImpl(newSetup, leaseDecorator, keepAliveProvider, errorConsumer);
    }

    @Override
    public SetupProvider metadataMimeType(String metadataMimeType) {
        Frame newSetup = from(getFlags(setupFrame), keepaliveInterval(setupFrame), maxLifetime(setupFrame),
            metadataMimeType, Frame.Setup.dataMimeType(setupFrame),
                new PayloadImpl(setupFrame));
        setupFrame.release();
        return new SetupProviderImpl(newSetup, leaseDecorator, keepAliveProvider, errorConsumer);
    }

    @Override
    public SetupProvider honorLease(Function<ReactiveSocket, LeaseHonoringSocket> leaseDecorator) {
        return new SetupProviderImpl(setupFrame, leaseDecorator, keepAliveProvider, errorConsumer);
    }

    @Override
    public SetupProvider disableLease() {
        return disableLease(DisableLeaseSocket::new);
    }

    @Override
    public SetupProvider disableLease(Function<ReactiveSocket, DisableLeaseSocket> socketFactory) {
        Frame newSetup = from(getFlags(setupFrame) & ~ConnectionSetupPayload.HONOR_LEASE,
            keepaliveInterval(setupFrame), maxLifetime(setupFrame),
            Frame.Setup.metadataMimeType(setupFrame), Frame.Setup.dataMimeType(setupFrame),
                new PayloadImpl(setupFrame));
        setupFrame.release();
        return new SetupProviderImpl(newSetup, socketFactory, keepAliveProvider, errorConsumer);
    }

    @Override
    public SetupProvider errorConsumer(Consumer<Throwable> errorConsumer) {
        return new SetupProviderImpl(setupFrame, leaseDecorator, keepAliveProvider, errorConsumer);
    }

    @Override
    public SetupProvider setupPayload(Payload setupPayload) {
        Frame newSetup = from(getFlags(setupFrame) & ~ConnectionSetupPayload.HONOR_LEASE,
            keepaliveInterval(setupFrame), maxLifetime(setupFrame),
            Frame.Setup.metadataMimeType(setupFrame), Frame.Setup.dataMimeType(setupFrame),
            setupPayload);
        setupFrame.release();
        return new SetupProviderImpl(newSetup, reactiveSocket -> new DisableLeaseSocket(reactiveSocket),
            keepAliveProvider, errorConsumer);
    }
}