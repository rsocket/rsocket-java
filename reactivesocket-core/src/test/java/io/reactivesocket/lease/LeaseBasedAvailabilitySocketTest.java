package io.reactivesocket.lease;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Flowable;
import org.agrona.BitUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class LeaseBasedAvailabilitySocketTest {
    @Test
    public void testConcurrency() {
        ByteBuffer buffer = ByteBuffer.allocate(BitUtil.SIZE_OF_INT);
        buffer.putInt(1);
        Lease lease = new LeaseImpl(2, (int) TimeUnit.HOURS.toMillis(1), buffer);
        ReactiveSocket rs = Mockito.mock(ReactiveSocket.class);

        Mockito.when(rs.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(Flowable.empty());

        Mockito.when(rs.availability()).thenReturn(1.0);

        LeaseBasedAvailabilitySocket socket = new LeaseBasedAvailabilitySocket(rs);
        socket.accept(lease);

        Assert.assertTrue("availability should be greater than zero",socket.availability() > 0);

        Publisher<Payload> hi = socket.requestResponse(new PayloadImpl("hi"));

        Assert.assertTrue("availability should be zero", socket.availability() <= 0);

        Flowable.fromPublisher(hi).blockingSubscribe();

        Assert.assertTrue("availability should be greater than zero",socket.availability() == 0.5);

        hi = socket.requestResponse(new PayloadImpl("hi"));
        Flowable.fromPublisher(hi).blockingSubscribe();
        Assert.assertTrue("availability should be zero", socket.availability() <= 0);
    }

    @Test
    public void testShoulBeZeroAvailibiltyWhenConcurrencyLimitIsLowerThanCocurrency() {
        ByteBuffer buffer = ByteBuffer.allocate(BitUtil.SIZE_OF_INT);
        buffer.putInt(10);
        Lease lease = new LeaseImpl(200, (int) TimeUnit.HOURS.toMillis(1), buffer);
        ReactiveSocket rs = Mockito.mock(ReactiveSocket.class);

        Mockito.when(rs.requestResponse(Mockito.any(Payload.class)))
            .thenReturn(Flowable.empty());

        Mockito.when(rs.availability()).thenReturn(1.0);

        LeaseBasedAvailabilitySocket socket = new LeaseBasedAvailabilitySocket(rs);
        socket.accept(lease);

        socket.requestResponse(new PayloadImpl("hi"));
        socket.requestResponse(new PayloadImpl("hi"));
        socket.requestResponse(new PayloadImpl("hi"));


        Assert.assertTrue("availability should be greater than zero",socket.availability() > 0);

        buffer = ByteBuffer.allocate(BitUtil.SIZE_OF_INT);
        buffer.putInt(3);
        lease = new LeaseImpl(200, (int) TimeUnit.HOURS.toMillis(1), buffer);
        socket.accept(lease);
        Assert.assertTrue("availability should be zero", socket.availability() == 0);

    }

}