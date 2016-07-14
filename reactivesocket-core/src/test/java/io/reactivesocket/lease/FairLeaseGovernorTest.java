package io.reactivesocket.lease;

import io.reactivesocket.Frame;
import io.reactivesocket.internal.Responder;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class FairLeaseGovernorTest {

    @Test(timeout = 10_000L)
    public void testAcceptRefuseLease() throws InterruptedException {
        int n = 10;
        FairLeaseGovernor governor = new FairLeaseGovernor(n, 100, TimeUnit.MILLISECONDS);
        Responder responder = mock(Responder.class);
        Frame frame = mock(Frame.class);

        governor.register(responder);
        Thread.sleep(10);

        assertTrue("First request is accepted", governor.accept(responder, frame));
        for (int i = 1; i < n; i++) {
            assertTrue("Subsequent requests are accepted", governor.accept(responder, frame));
        }
        assertFalse("11th request is refused", governor.accept(responder, frame));

        Thread.sleep(100);
        assertTrue("After some time, requests are accepted again", governor.accept(responder, frame));
    }

    @Test(timeout = 1000_000L)
    public void testLeaseFairness() throws InterruptedException {
        FairLeaseGovernor governor = new FairLeaseGovernor(4, 1000, TimeUnit.MILLISECONDS);
        Responder responder1 = mock(Responder.class);
        Responder responder2 = mock(Responder.class);
        Frame frame = mock(Frame.class);

        governor.register(responder1);
        governor.register(responder2);
        Thread.sleep(10);

        assertTrue("First request is accepted on responder 1", governor.accept(responder1, frame));
        assertTrue("First request is accepted on responder 2", governor.accept(responder2, frame));
        assertTrue("Second request is accepted on responder 1", governor.accept(responder1, frame));
        assertFalse("Third request is refused on responder 1", governor.accept(responder1, frame));
        assertTrue("Second request is accepted on responder 2", governor.accept(responder2, frame));
    }
}
