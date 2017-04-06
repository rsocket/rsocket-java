package io.reactivesocket.fragmentation;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import io.reactivesocket.internal.Int2ObjectHashMap;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Fragments and Re-assembles frames. MTU is number of bytes per fragment. The default is 1024
 */
public class FragmentionDuplexConnection implements DuplexConnection {

    private final int mtu;
    private final DuplexConnection source;
    private final Int2ObjectHashMap<FrameReassembler> frameReassemblers = new Int2ObjectHashMap<>();
    private final FrameFragmenter frameFragmenter;

    public FragmentionDuplexConnection(DuplexConnection source, int mtu) {
        this.mtu = mtu;
        this.source = source;
        this.frameFragmenter = new FrameFragmenter(mtu);
    }

    @Override
    public double availability() {
        return source.availability();
    }

    @Override
    public Mono<Void> send(Publisher<Frame> frames) {
        return Flux
            .from(frames)
            .concatMap(this::sendOne)
            .then();
    }

    @Override
    public Mono<Void> sendOne(Frame frame) {
        if (frameFragmenter.shouldFragment(frame)) {
            return source.send(frameFragmenter.fragment(frame));
        } else {
            return source.sendOne(frame);
        }
    }

    @Override
    public Flux<Frame> receive() {
        return source
            .receive()
            .concatMap(frame -> {
                if (FrameHeaderFlyweight.FLAGS_F == (frame.flags() & FrameHeaderFlyweight.FLAGS_F)) {
                    FrameReassembler frameReassembler = getFrameReassmbler(frame);
                    frameReassembler.append(frame);
                    return Mono.empty();
                } else if (frameReassemblersContain(frame.getStreamId())) {
                    FrameReassembler frameReassembler = removeFrameReassembler(frame.getStreamId());
                    frameReassembler.append(frame);
                    Frame reassembled = frameReassembler.reassemble();
                    return Mono.just(reassembled);
                } else {
                    return Mono.just(frame);
                }
            });
    }

    @Override
    public Mono<Void> close() {
        return source.close();
    }

    private synchronized FrameReassembler getFrameReassmbler(Frame frame) {
        return frameReassemblers.computeIfAbsent(frame.getStreamId(), s -> new FrameReassembler(frame));
    }

    private synchronized FrameReassembler removeFrameReassembler(int streamId) {
        return frameReassemblers.remove(streamId);
    }

    private synchronized boolean frameReassemblersContain(int streamId) {
        return frameReassemblers.containsKey(streamId);
    }

    @Override
    public Mono<Void> onClose() {
        return source.onClose().doFinally(s -> {
            synchronized (FragmentionDuplexConnection.this) {
                frameReassemblers
                    .values()
                    .forEach(FrameReassembler::dispose);

                frameReassemblers.clear();
            }
        });
    }
}
