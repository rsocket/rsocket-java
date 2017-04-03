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
        if (FrameHeaderFlyweight.FLAGS_M == (FrameHeaderFlyweight.FLAGS_M & frame.flags())) {
            final ByteBuf buffer = FrameHeaderFlyweight.sliceFrameMetadataRetained(frame.content());
            metadataBuffer.addComponent(true, buffer);
        } else {
            final ByteBuf buffer = FrameHeaderFlyweight.sliceFrameDataRetained(frame.content());
            dataBuffer.addComponent(true, buffer);
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
