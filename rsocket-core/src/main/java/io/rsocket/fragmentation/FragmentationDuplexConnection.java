package io.rsocket.fragmentation;

import io.netty.util.collection.IntObjectHashMap;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.frame.FrameHeaderFlyweight;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Fragments and Re-assembles frames. MTU is number of bytes per fragment. The default is 1024 */
public class FragmentationDuplexConnection implements DuplexConnection {

  private final DuplexConnection source;
  private final IntObjectHashMap<FrameReassembler> frameReassemblers = new IntObjectHashMap<>();
  private final FrameFragmenter frameFragmenter;

  public FragmentationDuplexConnection(DuplexConnection source, int mtu) {
    this.source = source;
    this.frameFragmenter = new FrameFragmenter(mtu);
  }

  public static int getDefaultMTU() {
    if (Boolean.getBoolean("io.rsocket.fragmentation.enable")) {
      return Integer.getInteger("io.rsocket.fragmentation.mtu", 1024);
    }

    return 0;
  }

  @Override
  public double availability() {
    return source.availability();
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    return Flux.from(frames).concatMap(this::sendOne).then();
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
        .concatMap(
            frame -> {
              if (FrameHeaderFlyweight.FLAGS_F == (frame.flags() & FrameHeaderFlyweight.FLAGS_F)) {
                FrameReassembler frameReassembler = getFrameReassembler(frame);
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

  private synchronized FrameReassembler getFrameReassembler(Frame frame) {
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
    return source
        .onClose()
        .doFinally(
            s -> {
              synchronized (FragmentationDuplexConnection.this) {
                frameReassemblers.values().forEach(FrameReassembler::dispose);

                frameReassemblers.clear();
              }
            });
  }
}
