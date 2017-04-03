package io.reactivesocket.fragmentation;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.util.PayloadImpl;
import org.junit.Test;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

public class FrameFragmenterTest {
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