/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.exceptions.TransportException;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class NettyDuplexConnection implements DuplexConnection {
    protected final Channel channel;
    protected final CopyOnWriteArrayList<Observer<Frame>> readers;

    protected NettyDuplexConnection(Channel channel, CopyOnWriteArrayList<Observer<Frame>> readers) {
        this.readers = readers;
        this.channel = channel;
    }

    @Override
    public final Observable<Frame> getInput() {
        return o -> {
            o.onSubscribe(() -> readers.removeIf(s -> s == o));
            readers.add(o);
        };
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o.subscribe(new Subscriber<Frame>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                // TODO: wire back pressure
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Frame frame) {
                try {
                    ByteBuf byteBuf = Unpooled.wrappedBuffer(frame.getByteBuffer());
                    ChannelFuture channelFuture = channel.writeAndFlush(byteBuf);
                    channelFuture.addListener(future -> {
                        Throwable cause = future.cause();
                        if (cause != null) {
                            if (cause instanceof ClosedChannelException) {
                                onError(new TransportException(cause));
                            } else {
                                onError(cause);
                            }
                        }
                    });
                } catch (Throwable t) {
                    onError(t);
                }
            }

            @Override
            public void onError(Throwable t) {
                callback.error(t);
                subscription.cancel();
            }

            @Override
            public void onComplete() {
                callback.success();
                subscription.cancel();
            }
        });
    }

    @Override
    public double availability() {
        return channel.isOpen() ? 1.0 : 0.0;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
