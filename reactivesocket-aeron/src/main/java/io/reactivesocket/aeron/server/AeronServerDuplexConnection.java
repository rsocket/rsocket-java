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
package io.reactivesocket.aeron.server;

import io.aeron.Publication;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.AeronUtil;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.aeron.internal.MessageType;
import io.reactivesocket.aeron.internal.NotConnectedException;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import org.agrona.BitUtil;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class AeronServerDuplexConnection implements DuplexConnection, Loggable {
    private final Publication publication;
    private final CopyOnWriteArrayList<Observer<Frame>> subjects;
    private volatile boolean isClosed;

    public AeronServerDuplexConnection(
        Publication publication) {
        this.publication = publication;
        this.subjects = new CopyOnWriteArrayList<>();
    }

    public List<? extends Observer<Frame>> getSubscriber() {
        return subjects;
    }

    @Override
    public final Observable<Frame> getInput() {
        if (isTraceEnabled()) {
            trace("-------getting input for publication session id {} ", publication.sessionId());
        }

        return new Observable<Frame>() {
            public void subscribe(Observer<Frame> o) {
                o.onSubscribe(new Disposable() {
                    @Override
                    public void dispose() {
                        if (isTraceEnabled()) {
                            trace("removing Observer for publication with session id {} ", publication.sessionId());
                        }

                        subjects.removeIf(s -> s == o);
                    }
                });

                subjects.add(o);
            }
        };
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o.subscribe(new ServerSubscription(publication, callback));
    }

    @Override
    public double availability() {
        return isClosed ? 0.0 : 1.0;
    }

    // TODO - this is bad - I need to queue this up somewhere and process this on the polling thread so it doesn't just block everything
    void ackEstablishConnection(int ackSessionId) {
        debug("Acking establish connection for session id => {}", ackSessionId);
        for (;;) {
            try {
                AeronUtil.tryClaimOrOffer(publication, (offset, buffer) -> {
                    buffer.putShort(offset, (short) 0);
                    buffer.putShort(offset + BitUtil.SIZE_OF_SHORT, (short) MessageType.ESTABLISH_CONNECTION_RESPONSE.getEncodedType());
                    buffer.putInt(offset + BitUtil.SIZE_OF_INT, ackSessionId);
                }, 2 * BitUtil.SIZE_OF_INT, Constants.SERVER_ACK_ESTABLISH_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                debug("Ack sent for session i => {}", ackSessionId);
            } catch (NotConnectedException ne) {
                continue;
            }
            break;
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() {
        isClosed = true;
        try {
            publication.close();
        } catch (Throwable t) {}
    }

    public String toString() {
        if (publication == null) {
            return  getClass().getName() + ":publication=null";
        }

        return getClass().getName() + ":publication=[" +
            "channel=" + publication.channel() + "," +
            "streamId=" + publication.streamId() + "," +
            "sessionId=" + publication.sessionId() + "]";

    }
}
