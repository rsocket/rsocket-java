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

import io.reactivesocket.ClientReactiveSocket;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ServerReactiveSocket;
import io.reactivesocket.StreamIdSupplier;
import io.reactivesocket.client.ReactiveSocketClient.SocketAcceptor;
import io.reactivesocket.internal.ClientServerInputMultiplexer;
import io.reactivesocket.lease.DisableLeaseSocket;
import io.reactivesocket.lease.LeaseEnforcingSocket;
import io.reactivesocket.lease.LeaseHonoringSocket;
import io.reactivesocket.util.PayloadImpl;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

import static io.reactivesocket.Frame.Setup.from;
import static io.reactivesocket.Frame.Setup.getFlags;
import static io.reactivesocket.Frame.Setup.keepaliveInterval;
import static io.reactivesocket.Frame.Setup.maxLifetime;

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