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
package io.reactivesocket.javax.websocket.client;

import io.reactivesocket.Frame;
import io.reactivesocket.javax.websocket.WebSocketDuplexConnection;
import org.glassfish.tyrus.client.ClientManager;
import org.glassfish.tyrus.client.ClientProperties;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.subjects.PublishSubject;

import javax.websocket.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

public class ReactiveSocketWebSocketClient extends Endpoint {
    private final PublishSubject<Frame> input = PublishSubject.create();
    private final Subscriber<? super WebSocketDuplexConnection> subscriber;

    public ReactiveSocketWebSocketClient(Subscriber<? super WebSocketDuplexConnection> subscriber) {
        this.subscriber = subscriber;
    }

    public Observable<Frame> getInput() {
        return input;
    }

    public static Publisher<WebSocketDuplexConnection> create(SocketAddress socketAddress, String path, ClientManager clientManager) {
        if (socketAddress instanceof InetSocketAddress) {
            InetSocketAddress address = (InetSocketAddress)socketAddress;
            try {
                return create(new URI("ws", null, address.getHostName(), address.getPort(), path, null, null), clientManager);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        } else {
            throw new IllegalArgumentException("unknown socket address type => " + socketAddress.getClass());
        }
    }

    public static Publisher<WebSocketDuplexConnection> create(URI uri, ClientManager clientManager) {
        return s -> {
            try {
                clientManager.getProperties().put(ClientProperties.RECONNECT_HANDLER, new ClientManager.ReconnectHandler() {
                    @Override
                    public boolean onConnectFailure(Exception exception) {
                        s.onError(exception);
                        return false;
                    }
                });
                ReactiveSocketWebSocketClient endpoint = new ReactiveSocketWebSocketClient(s);
                clientManager.asyncConnectToServer(endpoint, null, uri);
            } catch (DeploymentException e) {
                s.onError(e);
            }
        };
    }

    @Override
    public void onOpen(Session session, EndpointConfig config) {
        subscriber.onNext(new WebSocketDuplexConnection(session, input));
        subscriber.onComplete();
        session.addMessageHandler(new MessageHandler.Whole<ByteBuffer>() {
            @Override
            public void onMessage(ByteBuffer message) {
                Frame frame = Frame.from(message);
                input.onNext(frame);
            }
        });
    }

    @Override
    public void onClose(Session session, CloseReason closeReason) {
        input.onCompleted();
    }

    @Override
    public void onError(Session session, Throwable thr) {
        input.onError(thr);
    }
}
