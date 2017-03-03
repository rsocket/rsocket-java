package io.reactivesocket.frame;

import io.reactivesocket.FrameType;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class SetupFrameFlyweightTest {
    private final UnsafeBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

    @Test
    public void validFrame() {
        ByteBuffer metadata = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        ByteBuffer data = ByteBuffer.wrap(new byte[]{5, 4, 3});
        SetupFrameFlyweight.encode(directBuffer, 0, 0, 5, 500, "metadata_type", "data_type", metadata, data);

        metadata.rewind();
        data.rewind();

        assertEquals(FrameType.SETUP, FrameHeaderFlyweight.frameType(directBuffer, 0));
        assertEquals("metadata_type", SetupFrameFlyweight.metadataMimeType(directBuffer, 0));
        assertEquals("data_type", SetupFrameFlyweight.dataMimeType(directBuffer, 0));
        assertEquals(metadata, FrameHeaderFlyweight.sliceFrameMetadata(directBuffer, 0, FrameHeaderFlyweight.FRAME_HEADER_LENGTH));
        assertEquals(data, FrameHeaderFlyweight.sliceFrameData(directBuffer, 0, FrameHeaderFlyweight.FRAME_HEADER_LENGTH));
    }

    @Test(expected = IllegalArgumentException.class)
    public void resumeNotSupported() {
        SetupFrameFlyweight.encode(directBuffer, 0, SetupFrameFlyweight.FLAGS_RESUME_ENABLE, 5, 500, "", "", FrameHeaderFlyweight.NULL_BYTEBUFFER, FrameHeaderFlyweight.NULL_BYTEBUFFER);
    }

    @Test
    public void validResumeFrame() {
        ByteBuffer token = ByteBuffer.wrap(new byte[]{2, 3});
        ByteBuffer metadata = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        ByteBuffer data = ByteBuffer.wrap(new byte[]{5, 4, 3});
        SetupFrameFlyweight.encode(directBuffer, 0, SetupFrameFlyweight.FLAGS_RESUME_ENABLE, 5, 500, token, "metadata_type", "data_type", metadata, data);

        token.rewind();
        metadata.rewind();
        data.rewind();

        assertEquals(FrameType.SETUP, FrameHeaderFlyweight.frameType(directBuffer, 0));
        assertEquals("metadata_type", SetupFrameFlyweight.metadataMimeType(directBuffer, 0));
        assertEquals("data_type", SetupFrameFlyweight.dataMimeType(directBuffer, 0));
        assertEquals(metadata, FrameHeaderFlyweight.sliceFrameMetadata(directBuffer, 0, FrameHeaderFlyweight.FRAME_HEADER_LENGTH));
        assertEquals(data, FrameHeaderFlyweight.sliceFrameData(directBuffer, 0, FrameHeaderFlyweight.FRAME_HEADER_LENGTH));
        assertEquals(SetupFrameFlyweight.FLAGS_RESUME_ENABLE, FrameHeaderFlyweight.flags(directBuffer, 0) & SetupFrameFlyweight.FLAGS_RESUME_ENABLE);
    }
}