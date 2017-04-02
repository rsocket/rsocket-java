package io.reactivesocket.fragmentation;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.Plugins;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import io.reactivesocket.internal.Int2ObjectHashMap;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Fragments and Re-assembles frames. MTU is number of bytes per fragment. The default is 1024
 */
public class FragmentionDuplexConnection implements DuplexConnection {

    static {
        if (Boolean.getBoolean("io.reactivesocket.fragmentation.enable")) {
            int mtu = Integer.getInteger("io.reactivesocket.fragmentation.mtu", 1024);

            if (Plugins.DUPLEX_CONNECTION_INTERCEPTOR == null) {
                Plugins.DUPLEX_CONNECTION_INTERCEPTOR = (type, connection) -> {
                    if (type == Plugins.DuplexConnectionInterceptor.Type.SOURCE) {
                        return new FragmentionDuplexConnection(connection, mtu);
                    } else {
                        return connection;
                    }
                };
            } else {
                Plugins.DuplexConnectionInterceptor original = Plugins.DUPLEX_CONNECTION_INTERCEPTOR;
                Plugins.DUPLEX_CONNECTION_INTERCEPTOR = (type, connection) -> {
                    if (type == Plugins.DuplexConnectionInterceptor.Type.SOURCE) {
                        return original.apply(type, new FragmentionDuplexConnection(connection, mtu));
                    } else {
                        return original.apply(type, connection);
                    }
                };
            }
        }
    }

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
                    FrameReassembler frameReassembler = getFrameReassmbler(frame.getStreamId());
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

    private synchronized FrameReassembler getFrameReassmbler(int streamId) {
        return frameReassemblers.computeIfAbsent(streamId, s -> new FrameReassembler(mtu));
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
                    .forEach(FrameReassembler::clear);

                frameReassemblers.clear();
            }
        });
    }
}
