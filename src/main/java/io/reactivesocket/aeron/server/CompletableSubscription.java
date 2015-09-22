package io.reactivesocket.aeron.server;

import io.reactivesocket.Completable;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.AeronUtil;
import io.reactivesocket.aeron.internal.MessageType;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.BitUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rroeser on 8/27/15.
 */
public class CompletableSubscription implements Subscriber<Frame> {

    private final Publication publication;

    private final Completable completable;
    private volatile static AtomicLong count = new AtomicLong();
    private Subscription subscription;

    public CompletableSubscription(Publication publication, Completable completable, AutoCloseable closeable) {
        this.publication = publication;
        this.completable = completable;
    }

    @Override
    public void onSubscribe(Subscription s) {
        s.request(1);
        this.subscription = s;
    }

    @Override
    public void onNext(Frame frame) {
        final ByteBuffer byteBuffer = frame.getByteBuffer();
        final int length = frame.length() + BitUtil.SIZE_OF_INT;

        try {
            AeronUtil.tryClaimOrOffer(publication, (offset, buffer) -> {
                buffer.putShort(offset, getCount());
                buffer.putShort(offset + BitUtil.SIZE_OF_SHORT, (short) MessageType.FRAME.getEncodedType());
                buffer.putBytes(offset + BitUtil.SIZE_OF_INT, byteBuffer, frame.offset(), frame.length());
            }, length);
        } catch (Throwable t) {
            t.printStackTrace();
        }

        System.out.println("### Server Sending => " + frame);
    }

    @Override
    public void onError(Throwable t) {
        completable.error(t);
    }

    @Override
    public void onComplete() {
        System.out.println("&&&&&&&&&&&&&&&&& CompletableSubscription.onComplete");
        completable.success();
    }

    private short getCount() {
        return (short) count.incrementAndGet();
    }

}
