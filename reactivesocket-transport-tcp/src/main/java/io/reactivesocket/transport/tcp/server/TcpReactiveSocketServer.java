/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.reactivesocket.transport.tcp.server;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.DefaultReactiveSocket;
import io.reactivesocket.Frame;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.transport.tcp.ReactiveSocketFrameCodec;
import io.reactivesocket.transport.tcp.ReactiveSocketLengthCodec;
import io.reactivesocket.transport.tcp.TcpDuplexConnection;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.server.ConnectionHandler;
import io.reactivex.netty.protocol.tcp.server.TcpServer;
import rx.Completable;
import rx.Completable.CompletableOnSubscribe;
import rx.Completable.CompletableSubscriber;
import rx.Observable;

import java.net.SocketAddress;

public class TcpReactiveSocketServer {

    private final TcpServer<Frame, Frame> server;

    private TcpReactiveSocketServer(TcpServer<Frame, Frame> server) {
        this.server = server;
    }

    public StartedServer start(ConnectionSetupHandler setupHandler) {
        return start(setupHandler, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR);
    }

    public StartedServer start(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor) {
        server.start(new ConnectionHandler<Frame, Frame>() {
            @Override
            public Observable<Void> handle(Connection<Frame, Frame> newConnection) {
                TcpDuplexConnection c = new TcpDuplexConnection(newConnection);
                ReactiveSocket rs = DefaultReactiveSocket.fromServerConnection(c, setupHandler, leaseGovernor,
                                                                               Throwable::printStackTrace);
                return Completable.create(new CompletableOnSubscribe() {
                    @Override
                    public void call(CompletableSubscriber s) {
                        rs.start(new io.reactivesocket.rx.Completable() {
                            @Override
                            public void success() {
                                rs.onShutdown(new io.reactivesocket.rx.Completable() {
                                    @Override
                                    public void success() {
                                        s.onCompleted();
                                    }

                                    @Override
                                    public void error(Throwable e) {
                                        s.onError(e);
                                    }
                                });
                            }

                            @Override
                            public void error(Throwable e) {
                                s.onError(e);
                            }
                        });
                    }
                }).toObservable();
            }
        });

        return new StartedServer();
    }

    public static TcpReactiveSocketServer create() {
        return create(TcpServer.newServer());
    }

    public static TcpReactiveSocketServer create(int port) {
        return create(TcpServer.newServer(port));
    }

    public static TcpReactiveSocketServer create(TcpServer<ByteBuf, ByteBuf> rxNettyServer) {
        return new TcpReactiveSocketServer(configure(rxNettyServer));
    }

    private static TcpServer<Frame, Frame> configure(TcpServer<ByteBuf, ByteBuf> rxNettyServer) {
        return rxNettyServer.addChannelHandlerLast("line-codec", ReactiveSocketLengthCodec::new)
                            .addChannelHandlerLast("frame-codec", ReactiveSocketFrameCodec::new);
    }

    public final class StartedServer {

        public SocketAddress getServerAddress() {
            return server.getServerAddress();
        }

        public int getServerPort() {
            return server.getServerPort();
        }

        public void awaitShutdown() {
            server.awaitShutdown();
        }

        public void shutdown() {
            server.shutdown();
        }
    }
}
