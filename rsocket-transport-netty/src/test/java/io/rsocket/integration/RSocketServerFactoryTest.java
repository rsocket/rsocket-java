package io.rsocket.integration;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class RSocketServerFactoryTest {

    @Test
    public void closeReceiver() throws InterruptedException {
        int randomPort = ThreadLocalRandom.current().nextInt(10_000, 20_000);

        AtomicInteger results = new AtomicInteger();
        CountDownLatch errorLatch = new CountDownLatch(1);

        CloseableChannel closeableChannel = RSocketFactory.receive()
                .acceptor((setup, sendingSocket) -> {
                    return Mono.just(new AbstractRSocket() {

                        @Override
                        public Flux<Payload> requestStream(Payload payload) {
                            return Flux.interval(Duration.ofMillis(100))
                                    .onBackpressureDrop()
                                    .map(i -> DefaultPayload.create("Response-" +i));
                        }

                    });
                })
                .transport(TcpServerTransport.create(randomPort))
                .start()
                .block();

        RSocket sender = RSocketFactory.connect()
                .transport(TcpClientTransport.create(randomPort))
                .start()
                .block();

        sender.requestStream(DefaultPayload.create("Request"))
                .subscribe(new BaseSubscriber<Payload>() {

                    volatile Subscription subscription;

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        this.subscription = subscription;
                        this.subscription.request(1);
                        closeableChannel.onClose()
                                .doFinally(signalType-> {
                                    // request more, should produce an error as receiver no longer produce
                                    subscription.request(Integer.MAX_VALUE);
                                })
                                .subscribe();
                    }

                    @Override
                    protected void hookOnNext(Payload value) {
                        if (results.compareAndSet(0, 1)) {
                            // expecting it to stop the server
                            closeableChannel.dispose();
                        } else {
                            results.getAndIncrement();
                        }
                    }

                    @Override
                    protected void hookOnError(Throwable throwable) {
                        if (throwable instanceof ClosedChannelException) {
                            errorLatch.countDown();
                        }
                    }
                });

        assertThat(errorLatch.await(1, TimeUnit.SECONDS))
                .withFailMessage("ClosedChannelException is expected").isTrue();
        assertThat(results.get()).isEqualTo(1);
    }


}
