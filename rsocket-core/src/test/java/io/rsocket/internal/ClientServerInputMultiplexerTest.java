package io.rsocket.internal;

import io.rsocket.Frame;
import io.rsocket.test.util.TestDuplexConnection;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class ClientServerInputMultiplexerTest {
    private TestDuplexConnection source;
    private ClientServerInputMultiplexer multiplexer;

    @Before
    public void setup() {
        source = new TestDuplexConnection();
        multiplexer = new ClientServerInputMultiplexer(source);
    }

    @Test
    public void testSplits() {
        AtomicInteger clientFrames = new AtomicInteger();
        AtomicInteger serverFrames = new AtomicInteger();
        AtomicInteger connectionFrames = new AtomicInteger();

        multiplexer.asClientConnection().receive().doOnNext(f -> clientFrames.incrementAndGet()).subscribe();
        multiplexer.asServerConnection().receive().doOnNext(f -> serverFrames.incrementAndGet()).subscribe();
        multiplexer.asStreamZeroConnection().receive().doOnNext(f -> connectionFrames.incrementAndGet()).subscribe();

        source.addToReceivedBuffer(Frame.Error.from(1, new Exception()));
        assertEquals(1, clientFrames.get());
        assertEquals(0, serverFrames.get());
        assertEquals(0, connectionFrames.get());

        source.addToReceivedBuffer(Frame.Error.from(2, new Exception()));
        assertEquals(1, clientFrames.get());
        assertEquals(1, serverFrames.get());
        assertEquals(0, connectionFrames.get());

        source.addToReceivedBuffer(Frame.Error.from(1, new Exception()));
        assertEquals(2, clientFrames.get());
        assertEquals(1, serverFrames.get());
        assertEquals(0, connectionFrames.get());
    }
}
