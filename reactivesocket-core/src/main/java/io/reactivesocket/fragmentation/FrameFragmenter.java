package io.reactivesocket.fragmentation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import reactor.core.publisher.Flux;

import java.util.ArrayDeque;

/**
 *
 */
public class FrameFragmenter {
    private final int mtu;

    public FrameFragmenter(int mtu) {
        this.mtu = mtu;
    }

    public boolean shouldFragment(Frame frame) {
        return isFragmentableFrame(frame.getType()) && frame.content().capacity() > mtu;
    }

    private boolean isFragmentableFrame(FrameType type) {
        switch (type) {
            case FIRE_AND_FORGET:
            case REQUEST_STREAM:
            case REQUEST_CHANNEL:
            case REQUEST_RESPONSE:
            case PAYLOAD:
            case NEXT_COMPLETE:
            case METADATA_PUSH:
                return true;
            default:
                return false;
        }
    }

    ArrayDeque<ByteBuf> slice(ByteBuf byteBuf) {
        ArrayDeque<ByteBuf> slices = new ArrayDeque<>(byteBuf.capacity() / mtu);

        int capacity = byteBuf.capacity();
        for (int i = byteBuf.readerIndex(); i < capacity;) {
            int length = Math.min(capacity - i, mtu);
            slices.add(byteBuf.readSlice(length));
            i += mtu;
        }

        return slices;
    }

    public Flux<Frame> fragment(Frame frame) {
        frame.retain();
        final ByteBuf frameContent = frame.content();
        final ByteBuf metadata = FrameHeaderFlyweight.sliceFrameMetadata(frameContent);
        final ByteBuf data = FrameHeaderFlyweight.sliceFrameData(frameContent);

        Flux<Frame> metadataFlux;
        if (metadata.capacity() > 0) {
            ArrayDeque<ByteBuf> metadataSlices = slice(metadata);
            metadataFlux = fragment(frame, metadataSlices, data.capacity() > 0, true);
        } else {
            metadataFlux = Flux.empty();
        }

        Flux<Frame> dataFlux;
        if (data.capacity() > 0) {
            ArrayDeque<ByteBuf> dataSlices = slice(data);
            dataFlux = fragment(frame, dataSlices, true, false);
        } else {
            dataFlux = Flux.empty();
        }

        return Flux
            .concat(metadataFlux, dataFlux)
            .log()
            .doFinally(s -> {
                if (frame.refCnt() > 0) {
                    frame.release();
                }
            });
    }

    Flux<Frame> fragment(
        Frame frame,
        ArrayDeque<ByteBuf> slices,
        boolean lastFrame,
        boolean metadata) {
        return Flux
            .generate(
                sink -> {
                    if (!slices.isEmpty()) {
                        ByteBuf byteBuf = slices.poll();
                        int frameLength = metadata
                            ? FrameHeaderFlyweight.computeFrameHeaderLength(frame.getType(), byteBuf.capacity(), 0)
                            : FrameHeaderFlyweight.computeFrameHeaderLength(frame.getType(), 0, byteBuf.capacity());

                        ByteBuf content = PooledByteBufAllocator.DEFAULT.buffer(frameLength);

                        int flags = slices.isEmpty() && lastFrame
                            ? frame.flags()
                            : frame.flags() | FrameHeaderFlyweight.FLAGS_F;

                        if (metadata) {
                            FrameHeaderFlyweight.encode(content, frame.getStreamId(), flags, frame.getType(), byteBuf, Unpooled.EMPTY_BUFFER);
                        } else {
                            FrameHeaderFlyweight.encode(content, frame.getStreamId(), flags, frame.getType(), Unpooled.EMPTY_BUFFER, byteBuf);
                        }

                        sink.next(Frame.from(content));
                    } else {
                        sink.complete();
                    }
                });
    }

}
