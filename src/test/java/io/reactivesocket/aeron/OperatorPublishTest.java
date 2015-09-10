package io.reactivesocket.aeron;

import io.reactivesocket.Frame;
import rx.Subscriber;
import uk.co.real_logic.aeron.Publication;

import java.nio.ByteBuffer;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OperatorPublishTest {
    //@Test
    public void testShouldCallTryClaimWhenSmallerThanMTU() throws Exception {
        String message = "I'm a message longer than 1";
        ByteBuffer buffer = ByteBuffer.wrap(message.getBytes());

        Frame frame = mock(Frame.class);
        when(frame.getByteBuffer()).thenReturn(buffer);

        Publication publication = mock(Publication.class);
        when(publication.maxMessageLength()).thenReturn(1000);

        OperatorPublish publish = new OperatorPublish(publication);

        try {
            Subscriber subscriber = new Subscriber() {
                @Override
                public void onCompleted() {

                }

                @Override
                public void onError(Throwable e) {

                }

                @Override
                public void onNext(Object o) {

                }
            };

            Subscriber<? super Frame> call = publish.call(subscriber);
            call.onNext(frame);

        } catch (Throwable t) {
        }

        verify(publication, times(1)).tryClaim(anyInt(), anyObject());

    }

    //@Test
     public void testShouldCallOfferWhenLargerThenMTU() throws Exception {
        String message = "I'm a message longer than 1";
        ByteBuffer buffer = ByteBuffer.wrap(message.getBytes());

        Frame frame = mock(Frame.class);
        when(frame.getByteBuffer()).thenReturn(buffer);

        Publication publication = mock(Publication.class);
        when(publication.maxMessageLength()).thenReturn(1);

        OperatorPublish publish = new OperatorPublish(publication);

        try {
            Subscriber subscriber = new Subscriber() {
                @Override
                public void onCompleted() {

                }

                @Override
                public void onError(Throwable e) {
                }

                @Override
                public void onNext(Object o) {

                }
            };

            Subscriber<? super Frame> call = publish.call(subscriber);
            call.onNext(frame);

        } catch (Throwable t) {
        }

        verify(publication, times(1)).offer(anyObject());

    }
}