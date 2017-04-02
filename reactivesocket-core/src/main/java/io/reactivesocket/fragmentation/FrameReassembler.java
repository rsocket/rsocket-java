package io.reactivesocket.fragmentation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.reactivesocket.Frame;
import io.reactivesocket.frame.FrameHeaderFlyweight;

/**
 * Assembles Fragmented frames.
 */
public class FrameReassembler {
    private final int mtu;

    private Frame first;

    ByteBuf dataBuffer;
    ByteBuf metadataBuffer;

    public FrameReassembler(int mtu) {
        this.mtu = mtu;
        dataBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(mtu);
        metadataBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(mtu);
    }

    public synchronized void append(Frame frame) {
        if (first == null) {
            first = frame;
        }

        final ByteBuf content = frame.content();

        if (FrameHeaderFlyweight.FLAGS_M == (FrameHeaderFlyweight.FLAGS_M & frame.flags())) {
            final ByteBuf metadata = FrameHeaderFlyweight.sliceFrameMetadata(content);
            metadataBuffer.ensureWritable(metadata.readableBytes(), true);
            metadataBuffer.writeBytes(metadata);
        } else {
            final ByteBuf data = FrameHeaderFlyweight.sliceFrameData(content);
            dataBuffer.ensureWritable(data.readableBytes(), true);
            dataBuffer.writeBytes(data);
        }
    }

    public synchronized Frame reassemble() {
        try {
            int initialRequestN = Frame.Request.initialRequestN(first);
            Frame frame = Frame.Request.from(first.getStreamId(), first.getType(), metadataBuffer, dataBuffer, initialRequestN, first.flags());
            return frame;
        } finally {
            clear();
        }
    }

    public synchronized void clear() {
        if (dataBuffer.refCnt() > 0) {
            dataBuffer.release();
        }

        if (metadataBuffer.refCnt() > 0) {
            metadataBuffer.release();
        }

        dataBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(mtu);
        metadataBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(mtu);

        first = null;
    }

}
