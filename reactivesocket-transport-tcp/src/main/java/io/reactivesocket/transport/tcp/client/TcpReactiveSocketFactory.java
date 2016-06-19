/**
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
 */
package io.reactivesocket.transport.tcp.client;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.transport.tcp.ReactiveSocketFrameCodec;
import io.reactivesocket.transport.tcp.ReactiveSocketLengthCodec;
import io.reactivesocket.transport.tcp.TcpDuplexConnection;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.RxReactiveStreams;
import rx.Single;
import rx.Single.OnSubscribe;
import rx.SingleSubscriber;
import rx.Subscriber;

import java.net.SocketAddress;
import java.util.function.Consumer;

import static io.reactivesocket.DefaultReactiveSocket.*;

public class TcpReactiveSocketFactory implements ReactiveSocketFactory<SocketAddress> {

    private static final Logger logger = LoggerFactory.getLogger(TcpReactiveSocketFactory.class);

    private final SocketAddress socketAddress;
    private final TcpClient<Frame, Frame> client;
    private final ConnectionSetupPayload setup;
    private final Consumer<Throwable> errorStream;

    private TcpReactiveSocketFactory(TcpClient<ByteBuf, ByteBuf> client, SocketAddress socketAddress,
                                     ConnectionSetupPayload setupPayload, Consumer<Throwable> errorStream) {
        this.socketAddress = socketAddress;
        this.client = client.addChannelHandlerLast("length-codec", ReactiveSocketLengthCodec::new)
                            .addChannelHandlerLast("frame-codec", ReactiveSocketFrameCodec::new);
        setup = setupPayload;
        this.errorStream = errorStream;
    }

    @Override
    public Publisher<ReactiveSocket> apply() {
        Single<ReactiveSocket> r = Single.create(new OnSubscribe<ReactiveSocket>() {
            @Override
            public void call(SingleSubscriber<? super ReactiveSocket> s) {
                client.createConnectionRequest()
                      .toSingle()
                      .unsafeSubscribe(new Subscriber<Connection<Frame, Frame>>() {
                          @Override
                          public void onCompleted() {
                              // Single contract does not allow complete without onNext and onNext here completes
                              // the outer subscriber
                          }

                          @Override
                          public void onError(Throwable e) {
                              s.onError(e);
                          }

                          @Override
                          public void onNext(Connection<Frame, Frame> c) {
                              TcpDuplexConnection dc = new TcpDuplexConnection(c);
                              ReactiveSocket rs = fromClientConnection(dc, setup, errorStream);
                              rs.start(new Completable() {
                                  @Override
                                  public void success() {
                                      s.onSuccess(rs);
                                  }

                                  @Override
                                  public void error(Throwable e) {
                                      s.onError(e);
                                  }
                              });
                          }
                      });
            }
        });
        return RxReactiveStreams.toPublisher(r.toObservable());
    }

    @Override
    public double availability() {
        return 1.0;
    }

    @Override
    public SocketAddress remote() {
        return socketAddress;
    }

    public static TcpReactiveSocketFactory create(SocketAddress socketAddress, ConnectionSetupPayload setupPayload) {
        return create(socketAddress, TcpClient.newClient(socketAddress), setupPayload, new Consumer<Throwable>() {
            @Override
            public void accept(Throwable throwable) {
                logger.error("Error received on ReactiveSocket.", throwable);
            }
        });
    }

    public static TcpReactiveSocketFactory create(SocketAddress socketAddress,
                                                  ConnectionSetupPayload setupPayload,
                                                  Consumer<Throwable> errorStream) {
        return create(socketAddress, TcpClient.newClient(socketAddress), setupPayload, errorStream);
    }

    public static TcpReactiveSocketFactory create(SocketAddress socketAddress,
                                                  TcpClient<ByteBuf, ByteBuf> client,
                                                  ConnectionSetupPayload setupPayload,
                                                  Consumer<Throwable> errorStream) {
        return new TcpReactiveSocketFactory(client, socketAddress, setupPayload, errorStream);
    }
}
