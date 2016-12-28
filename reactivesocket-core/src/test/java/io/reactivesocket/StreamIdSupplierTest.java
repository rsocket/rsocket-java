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

        assertFalse(s.isValid(1));
        assertFalse(s.isValid(3));

        s.nextStreamId();
        assertTrue(s.isValid(1));
        assertFalse(s.isValid(3));

        s.nextStreamId();
        assertTrue(s.isValid(3));

        // negative
        assertFalse(s.isValid(-1));
        // connection
        assertFalse(s.isValid(0));
        // server
        assertFalse(s.isValid(2));
    }

    @Test
    public void testServerIsValid() {
        StreamIdSupplier s = StreamIdSupplier.serverSupplier();

        assertFalse(s.isValid(2));
        assertFalse(s.isValid(4));

        s.nextStreamId();
        assertTrue(s.isValid(2));
        assertFalse(s.isValid(4));

        s.nextStreamId();
        assertTrue(s.isValid(4));

        // negative
        assertFalse(s.isValid(-2));
        // connection
        assertFalse(s.isValid(0));
        // server
        assertFalse(s.isValid(1));
    }
}
