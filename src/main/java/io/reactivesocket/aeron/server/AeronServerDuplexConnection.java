package io.reactivesocket.aeron.server;

import io.reactivesocket.Completable;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.AeronDuplexConnectionSubject;
import io.reactivesocket.aeron.internal.AeronUtil;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.aeron.internal.MessageType;
import io.reactivesocket.observable.Observable;
import io.reactivesocket.observable.Observer;
import org.reactivestreams.Publisher;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.BitUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AeronServerDuplexConnection implements DuplexConnection, Loggable {
    private final Publication publication;
    private final ArrayList<AeronDuplexConnectionSubject> subjects;

    public AeronServerDuplexConnection(
        Publication publication) {
        this.publication = publication;
        this.subjects = new ArrayList<>();
    }

    public List<? extends Observer<Frame>> getSubscriber() {
        return subjects;
    }

    @Override
    public Observable<Frame> getInput() {
        AeronDuplexConnectionSubject subject = new AeronDuplexConnectionSubject(subjects);
        subjects.add(subject);
        return subject;
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o.subscribe(new ServerSubscription(publication, callback));
    }

    void ackEstablishConnection(int ackSessionId) {
        debug("Acking establish connection for session id => {}", ackSessionId);
        AeronUtil.tryClaimOrOffer(publication, (offset, buffer) -> {
            buffer.putShort(offset, (short) 0);
            buffer.putShort(offset + BitUtil.SIZE_OF_SHORT, (short) MessageType.ESTABLISH_CONNECTION_RESPONSE.getEncodedType());
            buffer.putInt(offset + BitUtil.SIZE_OF_INT, ackSessionId);
        }, 2 * BitUtil.SIZE_OF_INT, 30, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        publication.close();
    }
}
