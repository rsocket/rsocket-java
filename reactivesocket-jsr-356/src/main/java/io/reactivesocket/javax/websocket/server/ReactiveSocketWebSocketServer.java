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
package io.reactivesocket.javax.websocket.server;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.Frame;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.javax.websocket.WebSocketDuplexConnection;
import rx.subjects.PublishSubject;
import uk.co.real_logic.agrona.LangUtil;

import javax.websocket.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class ReactiveSocketWebSocketServer extends Endpoint {
    private final PublishSubject<Frame> input;
    private final ConcurrentHashMap<String, ReactiveSocket> reactiveSockets;
    private final ConnectionSetupHandler setupHandler;
    private final LeaseGovernor leaseGovernor;

    protected ReactiveSocketWebSocketServer(ConnectionSetupHandler setupHandler, LeaseGovernor leaseGovernor) {
        this.input = PublishSubject.create();
        this.reactiveSockets = new ConcurrentHashMap<>();
        this.setupHandler = setupHandler;
        this.leaseGovernor = leaseGovernor;
    }

    protected ReactiveSocketWebSocketServer(ConnectionSetupHandler setupHandler) {
        this(setupHandler, LeaseGovernor.UNLIMITED_LEASE_GOVERNOR);
    }

    @Override
    public void onOpen(Session session, EndpointConfig config) {
        session.addMessageHandler(new MessageHandler.Whole<ByteBuffer>() {
            @Override
            public void onMessage(ByteBuffer message) {
                Frame frame = Frame.from(message);
                input.onNext(frame);
            }
        });

        WebSocketDuplexConnection webSocketDuplexConnection = WebSocketDuplexConnection.create(session, input);

        final ReactiveSocket reactiveSocket = reactiveSockets.computeIfAbsent(session.getId(), id ->
            ReactiveSocket.fromServerConnection(
                webSocketDuplexConnection,
                setupHandler,
                leaseGovernor,
                t -> t.printStackTrace()
            )
        );

        reactiveSocket.start(new Completable() {
            @Override
            public void success() {
            }

            @Override
            public void error(Throwable e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void onClose(Session session, CloseReason closeReason) {
        input.onCompleted();
        try {
            ReactiveSocket reactiveSocket = reactiveSockets.remove(session.getId());
            if (reactiveSocket != null) {
                reactiveSocket.close();
            }
        } catch (Exception e) {
            LangUtil.rethrowUnchecked(e);
        }
    }

    @Override
    public void onError(Session session, Throwable thr) {
        input.onError(thr);
    }
}
