package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.FrameType;
import org.junit.Test;

import static org.junit.Assert.*;

public class SetupFrameFlyweightTest {
    private final ByteBuf byteBuf = Unpooled.buffer(1024);

    @Test
    public void validFrame() {
        ByteBuf metadata = Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4});
        ByteBuf data = Unpooled.wrappedBuffer(new byte[]{5, 4, 3});
        SetupFrameFlyweight.encode(byteBuf, 0, 5, 500, "metadata_type", "data_type", metadata, data);

        metadata.resetReaderIndex();
        data.resetReaderIndex();

        assertEquals(FrameType.SETUP, FrameHeaderFlyweight.frameType(byteBuf));
        assertEquals("metadata_type", SetupFrameFlyweight.metadataMimeType(byteBuf));
        assertEquals("data_type", SetupFrameFlyweight.dataMimeType(byteBuf));
        assertEquals(metadata, FrameHeaderFlyweight.sliceFrameMetadata(byteBuf));
        assertEquals(data, FrameHeaderFlyweight.sliceFrameData(byteBuf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void resumeNotSupported() {
        SetupFrameFlyweight.encode(byteBuf, SetupFrameFlyweight.FLAGS_RESUME_ENABLE, 5, 500, "", "", Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
    }

    @Test
    public void validResumeFrame() {
        ByteBuf token = Unpooled.wrappedBuffer(new byte[]{2, 3});
        ByteBuf metadata = Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4});
        ByteBuf data = Unpooled.wrappedBuffer(new byte[]{5, 4, 3});
        SetupFrameFlyweight.encode(byteBuf, SetupFrameFlyweight.FLAGS_RESUME_ENABLE, 5, 500, token, "metadata_type", "data_type", metadata, data);

        token.resetReaderIndex();
        metadata.resetReaderIndex();
        data.resetReaderIndex();

        assertEquals(FrameType.SETUP, FrameHeaderFlyweight.frameType(byteBuf));
        assertEquals("metadata_type", SetupFrameFlyweight.metadataMimeType(byteBuf));
        assertEquals("data_type", SetupFrameFlyweight.dataMimeType(byteBuf));
        assertEquals(metadata, FrameHeaderFlyweight.sliceFrameMetadata(byteBuf));
        assertEquals(data, FrameHeaderFlyweight.sliceFrameData(byteBuf));
        assertEquals(SetupFrameFlyweight.FLAGS_RESUME_ENABLE, FrameHeaderFlyweight.flags(byteBuf) & SetupFrameFlyweight.FLAGS_RESUME_ENABLE);
    }
}