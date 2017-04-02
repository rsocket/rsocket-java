package io.reactivesocket.fragmentation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import io.reactivesocket.util.PayloadImpl;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class FrameFragmenterTest {
    @Test
    public void testSlice() {
        ByteBuffer data = createRandomBytes(16);
        FrameFragmenter frameFragmenter = new FrameFragmenter(2);

        System.out.println(ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(data)));

        ArrayDeque<ByteBuf> slice = frameFragmenter.slice(Unpooled.wrappedBuffer(data));

        Assert.assertEquals(slice.size(), 8);

        CompositeByteBuf byteBufs = Unpooled.compositeBuffer();

        slice
            .forEach(byteBuf -> {
                byteBufs.addComponent(true, byteBuf);
            });

        System.out.println(ByteBufUtil.prettyHexDump(byteBufs));


        for (int i = 0; i < data.capacity(); i++) {
            Assert.assertEquals(data.get(i), byteBufs.getByte(i));

        }
    }

    @Test
    public void testSliceWithOddNumber() {
        ByteBuffer data = createRandomBytes(17);
        FrameFragmenter frameFragmenter = new FrameFragmenter(2);

        System.out.println(ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(data)));

        ArrayDeque<ByteBuf> slice = frameFragmenter.slice(Unpooled.wrappedBuffer(data));

        Assert.assertEquals(slice.size(), 9);

        CompositeByteBuf byteBufs = Unpooled.compositeBuffer();

        slice
            .forEach(byteBuf -> {
                byteBufs.addComponent(true, byteBuf);
            });

        System.out.println(ByteBufUtil.prettyHexDump(byteBufs));


        for (int i = 0; i < data.capacity(); i++) {
            Assert.assertEquals(data.get(i), byteBufs.getByte(i));

        }
    }


    @Test
    public void testFragmentData() {
        ByteBuffer data = createRandomBytes(16);
        ByteBuffer metadata = createRandomBytes(16);
        FrameFragmenter frameFragmenter = new FrameFragmenter(2);
        Frame from = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);

        ArrayDeque<ByteBuf> slice = frameFragmenter.slice(Unpooled.wrappedBuffer(data));

        Flux<Frame> frameFlux = frameFragmenter.fragment(from, slice, true, false);

        StepVerifier
            .create(frameFlux)
            .expectNextCount(7)
            .assertNext(frame -> {
                Assert.assertTrue((frame.flags() & FrameHeaderFlyweight.FLAGS_F) == 0);
            })
            .verifyComplete();
    }

    @Test
    public void testFragmentMetadata() {
        ByteBuffer data = createRandomBytes(16);
        ByteBuffer metadata = createRandomBytes(16);
        FrameFragmenter frameFragmenter = new FrameFragmenter(2);
        Frame from = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);

        ArrayDeque<ByteBuf> slice = frameFragmenter.slice(Unpooled.wrappedBuffer(data));

        Flux<Frame> frameFlux = frameFragmenter.fragment(from, slice, false, true);

        StepVerifier
            .create(frameFlux)
            .expectNextCount(7)
            .assertNext(frame -> {
                Assert.assertFalse((frame.flags() & FrameHeaderFlyweight.FLAGS_F) == 0);
            })
            .verifyComplete();
    }

    @Test
    public void testFragmentWithMetadataAndData() {
        ByteBuffer data = createRandomBytes(16);
        ByteBuffer metadata = createRandomBytes(16);

        Frame from = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);

        FrameFragmenter frameFragmenter = new FrameFragmenter(2);

        StepVerifier
            .create(frameFragmenter.fragment(from))
            .expectNextCount(16)
            .verifyComplete();
    }

    @Test
    public void testFragmentWithMetadataAndDataWithOddData() {
        ByteBuffer data = createRandomBytes(17);
        ByteBuffer metadata = createRandomBytes(17);

        Frame from = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);

        FrameFragmenter frameFragmenter = new FrameFragmenter(2);

        StepVerifier
            .create(frameFragmenter.fragment(from))
            .expectNextCount(18)
            .verifyComplete();

    }

    @Test
    public void testFragmentWithMetadataOnly() {
        ByteBuffer data = ByteBuffer.allocate(0);
        ByteBuffer metadata = createRandomBytes(16);

        Frame from = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);

        FrameFragmenter frameFragmenter = new FrameFragmenter(2);

        StepVerifier
            .create(frameFragmenter.fragment(from))
            .expectNextCount(8)
            .verifyComplete();
    }

    @Test
    public void testFragmentWithDdataOnly() {
        ByteBuffer data = createRandomBytes(16);
        ByteBuffer metadata = ByteBuffer.allocate(0);

        Frame from = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);

        FrameFragmenter frameFragmenter = new FrameFragmenter(2);

        StepVerifier
            .create(frameFragmenter.fragment(from))
            .expectNextCount(8)
            .verifyComplete();
    }

    private ByteBuffer createRandomBytes(int size) {
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }
}