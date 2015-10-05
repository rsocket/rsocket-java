package io.reactivesocket.aeron.internal;

import org.junit.Test;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AeronUtilTest {

    @Test(expected = TimedOutException.class)
    public void testOfferShouldTimeOut() {
        Publication publication = mock(Publication.class);
        AeronUtil.BufferFiller bufferFiller = mock(AeronUtil.BufferFiller.class);

        when(publication.offer(any(DirectBuffer.class))).thenReturn(Publication.BACK_PRESSURED);

        AeronUtil
            .offer(publication, bufferFiller, 1, 100, TimeUnit.MILLISECONDS);

    }

    @Test(expected = TimedOutException.class)
    public void testTryClaimShouldTimeOut() {
        Publication publication = mock(Publication.class);
        AeronUtil.BufferFiller bufferFiller = mock(AeronUtil.BufferFiller.class);

        when(publication.tryClaim(anyInt(), any(BufferClaim.class)))
            .thenReturn(Publication.BACK_PRESSURED);

        AeronUtil
            .tryClaim(publication, bufferFiller, 1, 100, TimeUnit.MILLISECONDS);

    }
}