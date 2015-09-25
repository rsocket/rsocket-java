package io.reactivesocket.aeron.server;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.AeronUtil;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.aeron.internal.MessageType;
import io.reactivesocket.aeron.internal.NotConnectedException;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.BitUtil;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AeronServerDuplexConnection implements DuplexConnection, Loggable {
    protected final static AtomicLong count = new AtomicLong();

    private final Publication publication;
    private final CopyOnWriteArrayList<Observer<Frame>> subjects;

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
        return new Observable<Frame>() {
            public void subscribe(Observer<Frame> o) {
                o.onSubscribe(new Disposable() {
                    @Override
                    public void dispose() {
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

    void ackEstablishConnection(int ackSessionId) {
        debug("Acking establish connection for session id => {}", ackSessionId);
        for (int i = 0; i < 5; i++) {
            try {
                AeronUtil.tryClaimOrOffer(publication, (offset, buffer) -> {
                    buffer.putShort(offset, (short) 0);
                    buffer.putShort(offset + BitUtil.SIZE_OF_SHORT, (short) MessageType.ESTABLISH_CONNECTION_RESPONSE.getEncodedType());
                    buffer.putInt(offset + BitUtil.SIZE_OF_INT, ackSessionId);
                }, 2 * BitUtil.SIZE_OF_INT, 30, TimeUnit.SECONDS);
                break;
            } catch (NotConnectedException ne) {
                if (i >= 4) {
                    throw ne;
                }
            }
        }
    }

    @Override
    public void close() {
        publication.close();
    }
}
