package io.reactivesocket;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamIdSupplierTest {
    @Test
    public void testClientSequence() {
        StreamIdSupplier s = StreamIdSupplier.clientSupplier();
        assertEquals(1, s.nextStreamId());
        assertEquals(3, s.nextStreamId());
        assertEquals(5, s.nextStreamId());
    }

    @Test
    public void testServerSequence() {
        StreamIdSupplier s = StreamIdSupplier.serverSupplier();
        assertEquals(2, s.nextStreamId());
        assertEquals(4, s.nextStreamId());
        assertEquals(6, s.nextStreamId());
    }

    @Test
    public void testClientIsValid() {
        StreamIdSupplier s = StreamIdSupplier.clientSupplier();

        assertFalse(s.isBeforeOrCurrent(1));
        assertFalse(s.isBeforeOrCurrent(3));

        s.nextStreamId();
        assertTrue(s.isBeforeOrCurrent(1));
        assertFalse(s.isBeforeOrCurrent(3));

        s.nextStreamId();
        assertTrue(s.isBeforeOrCurrent(3));

        // negative
        assertFalse(s.isBeforeOrCurrent(-1));
        // connection
        assertFalse(s.isBeforeOrCurrent(0));
        // server also accepted (checked externally)
        assertTrue(s.isBeforeOrCurrent(2));
    }

    @Test
    public void testServerIsValid() {
        StreamIdSupplier s = StreamIdSupplier.serverSupplier();

        assertFalse(s.isBeforeOrCurrent(2));
        assertFalse(s.isBeforeOrCurrent(4));

        s.nextStreamId();
        assertTrue(s.isBeforeOrCurrent(2));
        assertFalse(s.isBeforeOrCurrent(4));

        s.nextStreamId();
        assertTrue(s.isBeforeOrCurrent(4));

        // negative
        assertFalse(s.isBeforeOrCurrent(-2));
        // connection
        assertFalse(s.isBeforeOrCurrent(0));
        // client also accepted (checked externally)
        assertTrue(s.isBeforeOrCurrent(1));
    }
}
