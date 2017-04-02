package io.reactivesocket.fragmentation;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.util.PayloadImpl;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
public class FragmentionDuplexConnectionTest {
    @Test
    public void testSendOneWithFragmentation() {
        DuplexConnection mockConnection = mock(DuplexConnection.class);
        when(mockConnection
            .send(any()))
            .then(invocation -> {
                Publisher<Frame> frames = invocation.getArgumentAt(0, Publisher.class);

                StepVerifier
                    .create(frames)
                    .expectNextCount(16)
                    .verifyComplete();

                return Mono.empty();
            });
        when(mockConnection.sendOne(any(Frame.class))).thenReturn(Mono.empty());

        ByteBuffer data = createRandomBytes(16);
        ByteBuffer metadata = createRandomBytes(16);

        Frame frame = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);


        FragmentionDuplexConnection duplexConnection = new FragmentionDuplexConnection(mockConnection, 2);

        StepVerifier
            .create(duplexConnection.sendOne(frame))
            .verifyComplete();
    }

    @Test
    public void testShouldNotFragment() {
        DuplexConnection mockConnection = mock(DuplexConnection.class);
        when(mockConnection.sendOne(any(Frame.class))).thenReturn(Mono.empty());

        ByteBuffer data = createRandomBytes(16);
        ByteBuffer metadata = createRandomBytes(16);

        Frame frame = Frame.Cancel.from(1);

        FragmentionDuplexConnection duplexConnection = new FragmentionDuplexConnection(mockConnection, 2);

        StepVerifier
            .create(duplexConnection.sendOne(frame))
            .verifyComplete();

        verify(mockConnection, times(1)).sendOne(frame);
    }

    @Test
    public void testShouldFragmentMultiple() {
        DuplexConnection mockConnection = mock(DuplexConnection.class);
        when(mockConnection
            .send(any()))
            .then(invocation -> {
                Publisher<Frame> frames = invocation.getArgumentAt(0, Publisher.class);

                StepVerifier
                    .create(frames)
                    .expectNextCount(16)
                    .verifyComplete();

                return Mono.empty();
            });
        when(mockConnection.sendOne(any(Frame.class))).thenReturn(Mono.empty());

        ByteBuffer data = createRandomBytes(16);
        ByteBuffer metadata = createRandomBytes(16);

        Frame frame1 = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);
        Frame frame2 = Frame.Request.from(2, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);
        Frame frame3 = Frame.Request.from(3, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);

        FragmentionDuplexConnection duplexConnection = new FragmentionDuplexConnection(mockConnection, 2);

        StepVerifier
            .create(duplexConnection.send(Flux.just(frame1, frame2, frame3)))
            .verifyComplete();

        verify(mockConnection, times(3)).send(any());
    }

    @Test
    public void testReassembleFragmentFrame() {
        ByteBuffer data = createRandomBytes(16);
        ByteBuffer metadata = createRandomBytes(16);
        Frame frame = Frame.Request.from(1024, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);
        FrameFragmenter frameFragmenter = new FrameFragmenter(2);
        Flux<Frame> fragmentedFrames = frameFragmenter.fragment(frame);
        EmitterProcessor processor = EmitterProcessor.create(128);
        DuplexConnection mockConnection = mock(DuplexConnection.class);
        when(mockConnection.receive()).then(answer -> processor);

        FragmentionDuplexConnection duplexConnection  = new FragmentionDuplexConnection(mockConnection, 2);

        fragmentedFrames
            .subscribe(processor);

        duplexConnection
            .receive()
            .log()
            .doOnNext(c -> System.out.println("here - " + c.toString()))
            .subscribe();
    }

    private ByteBuffer createRandomBytes(int size) {
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }
}