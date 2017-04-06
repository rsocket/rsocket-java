package io.reactivesocket.fragmentation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import reactor.core.Disposable;

/**
 * Assembles Fragmented frames.
 */
public class FrameReassembler implements Disposable {
    private final FrameType frameType;
    private final int streamId;
    private final int flags;
    private final CompositeByteBuf dataBuffer;
    private final CompositeByteBuf metadataBuffer;

    public FrameReassembler(Frame frame) {
        this.frameType = frame.getType();
        this.streamId = frame.getStreamId();
        this.flags = frame.flags();
        dataBuffer = PooledByteBufAllocator.DEFAULT.compositeBuffer();
        metadataBuffer = PooledByteBufAllocator.DEFAULT.compositeBuffer();
    }

    public synchronized void append(Frame frame) {
        final ByteBuf byteBuf = frame.content();
        final FrameType frameType = FrameHeaderFlyweight.frameType(byteBuf);
        final int frameLength = FrameHeaderFlyweight.frameLength(byteBuf);
        final int metadataLength = FrameHeaderFlyweight.metadataLength(byteBuf, frameType, frameLength);
        final int dataLength = FrameHeaderFlyweight.dataLength(byteBuf, frameType);
        if (0 < metadataLength) {
            int metadataOffset = FrameHeaderFlyweight.metadataOffset(byteBuf);
            if (FrameHeaderFlyweight.hasMetadataLengthField(frameType)) {
                metadataOffset += FrameHeaderFlyweight.FRAME_LENGTH_SIZE;
            }
            metadataBuffer.addComponent(true, byteBuf.retainedSlice(metadataOffset, metadataLength));
        }
        if (0 < dataLength) {
            final int dataOffset = FrameHeaderFlyweight.dataOffset(byteBuf, frameType, frameLength);
            dataBuffer.addComponent(true, byteBuf.retainedSlice(dataOffset, dataLength));
        }
    }

    public synchronized Frame reassemble() {
        return Frame.PayloadFrame.from(streamId, frameType, metadataBuffer, dataBuffer, flags);
    }

    @Override
    public void dispose() {
        dataBuffer.release();
        metadataBuffer.release();
    }
}
