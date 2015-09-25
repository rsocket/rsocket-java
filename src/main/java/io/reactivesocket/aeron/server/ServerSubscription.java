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

import io.reactivesocket.rx.Completable;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.AeronUtil;
import io.reactivesocket.aeron.internal.MessageType;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.BitUtil;

import java.nio.ByteBuffer;

/**
 * Subscription used by the AeronServerDuplexConnection to handle incoming frames and send them
 * on a publication.
 *
 * @see io.reactivesocket.aeron.server.AeronServerDuplexConnection
 */
class ServerSubscription implements Subscriber<Frame> {

    /**
     * Count is used to by the client to round-robin request between threads.
     */
    private short count;

    private final Publication publication;

    private final Completable completable;

    public ServerSubscription(Publication publication, Completable completable) {
        this.publication = publication;
        this.completable = completable;
    }

    @Override
    public void onSubscribe(Subscription s) {
        s.request(1);
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
            onError(t);
        }

    }

    @Override
    public void onError(Throwable t) {
        completable.error(t);
    }

    @Override
    public void onComplete() {
        completable.success();
    }

    private short getCount() {
        return count++;
    }

}
