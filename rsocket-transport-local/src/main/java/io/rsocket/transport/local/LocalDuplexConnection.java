package io.rsocket.transport.local;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class LocalDuplexConnection implements DuplexConnection {
    private final Flux<Frame> in;
    private final Subscriber<Frame> out;
    private final MonoProcessor<Void> closeNotifier;

    public LocalDuplexConnection(Flux<Frame> in, Subscriber<Frame> out, MonoProcessor<Void> closeNotifier) {
        this.in = in;
        this.out = out;
        this.closeNotifier = closeNotifier;
    }

    @Override
    public Mono<Void> send(Publisher<Frame> frames) {
        return Flux.from(frames)
            .concatMap(this::sendOne)
            .then();
    }

    @Override
    public Mono<Void> sendOne(Frame frame) {
        return Mono.fromRunnable(() ->
            out.onNext(frame)
        );
    }

    @Override
    public Flux<Frame> receive() {
        return in;
    }

    @Override
    public Mono<Void> close() {
        return Mono.defer(() -> {
            out.onComplete();
            closeNotifier.onComplete();
            return closeNotifier;
        });
    }

    @Override
    public Mono<Void> onClose() {
        return closeNotifier;
    }

    @Override
    public double availability() {
        return closeNotifier.isDisposed() ? 0.0 : 1.0;
    }

}
