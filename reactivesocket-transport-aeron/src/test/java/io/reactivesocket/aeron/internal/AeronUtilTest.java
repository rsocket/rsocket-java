/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.aeron.internal;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.junit.Test;

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