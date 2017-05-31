package io.rsocket.integration;

import io.rsocket.*;
import io.rsocket.stat.BaseStats;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.junit.Test;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.SignalType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;


/**
 *
 */
public class RSocketStatsTest {

    public static class TestStats extends BaseStats {
        static class FrameInfo {
            private String payload;
            private String metadata;
            private int streamId;
            private FrameType type;

            public FrameInfo(int streamId, FrameType type, String payload, String metadata) {
                this.streamId = streamId;
                this.type = type;
                this.payload = payload;
                this.metadata = metadata;
            }

            public void match(Frame frame) {
                assertThat("Unexpected StreamId", frame.getStreamId(), is(streamId));
                assertThat("Unexpected FrameType.", frame.getType(), is(type));
                assertThat("Unexpected data.", byteToString(frame.getData()), equalTo(payload));
                assertThat("Unexpected metadata.", byteToString(frame.getMetadata()), equalTo(metadata));
            }

            private static String byteToString(final ByteBuffer byteBuffer)
            {
                return StandardCharsets.UTF_8.decode(byteBuffer).toString();
            }
        };

        public SignalType expectedSocketClosedSignalType = SignalType.ON_COMPLETE;
        public List<FrameInfo> expectedFrameReads = null;
        public int currentReadFrameIndex = 0;
        public List<FrameInfo> expectedFrameWrites = null;
        public int currentWriteFrameIndex = 0;
        public int expectedSocketCloseCount = -1;
        public MonoProcessor<Void> onAllSocketsClosed;

        public int createdSocketCount;
        public int closedSocketCount;
        public int disconnectedSocketCount;
        public int readFrameCount;
        public int writtenFrameCount;
        public int createdDuplexConnectionCount;
        public int closedDuplexConnectionCount;

        @Override
        public void socketCreated() {
            ++createdSocketCount;
        }

        @Override
        public void socketClosed(SignalType signalType) {
            assertThat("Socket close signal type", signalType, is(expectedSocketClosedSignalType));
            ++closedSocketCount;
            if (closedSocketCount == expectedSocketCloseCount) {
                if (onAllSocketsClosed != null) {
                    onAllSocketsClosed.onComplete();
                }
            }
        }

        @Override
        public void socketDisconnected() {
            ++disconnectedSocketCount;
        }

        @Override
        public void frameRead(Frame frame) {
            if (expectedFrameReads != null) {
                assertThat("More read frames than expected",
                        currentReadFrameIndex,
                        lessThan(expectedFrameReads.size()));
                expectedFrameReads.get(currentReadFrameIndex).match(frame);
                ++currentReadFrameIndex;
            }
            ++readFrameCount;
        }

        @Override
        public void frameWritten(Frame frame) {
            if (expectedFrameWrites != null) {
                assertThat("More write frames than expected",
                        currentWriteFrameIndex,
                        lessThan(expectedFrameWrites.size()));
                expectedFrameWrites.get(currentWriteFrameIndex).match(frame);
                ++currentWriteFrameIndex;
            }
            ++writtenFrameCount;
        }

        @Override
        public void duplexConnectionCreated(DuplexConnection connection) {
            ++createdDuplexConnectionCount;
        }

        @Override
        public void duplexConnectionClosed(DuplexConnection connection) {
            ++closedDuplexConnectionCount;
        }
    }

    @Test(timeout = 2000)
    public void testStreamingClient() throws InterruptedException {
        TestStats stats = new TestStats();
        stats.expectedFrameReads = new ArrayList<>(4);
        stats.expectedFrameReads.add(new TestStats.FrameInfo(0, FrameType.SETUP, "", ""));
        stats.expectedFrameReads.add(new TestStats.FrameInfo(1, FrameType.REQUEST_STREAM, "Hello", ""));
        stats.expectedFrameReads.add(new TestStats.FrameInfo(1, FrameType.NEXT, "RESPONSE", "METADATA"));
        stats.expectedFrameReads.add(new TestStats.FrameInfo(1, FrameType.COMPLETE, "", ""));

        stats.expectedFrameWrites = new ArrayList<>(3);
        stats.expectedFrameWrites.add(new TestStats.FrameInfo(1, FrameType.REQUEST_STREAM, "Hello", ""));
        stats.expectedFrameWrites.add(new TestStats.FrameInfo(1, FrameType.NEXT, "RESPONSE", "METADATA"));
        stats.expectedFrameWrites.add(new TestStats.FrameInfo(1, FrameType.COMPLETE, "", ""));

        stats.expectedSocketCloseCount = 2;
        stats.onAllSocketsClosed = MonoProcessor.create();

        RSocketFactory
                .receive()
                .stats(stats)
                .acceptor(new SocketAcceptorImpl())
                .transport(TcpServerTransport.create("localhost", 7000))
                .start()
                .subscribe();

        RSocket socket = RSocketFactory
                .connect()
                .stats(stats)
                .transport(TcpClientTransport.create("localhost", 7000))
                .start()
                .block();

        socket.requestStream(new PayloadImpl("Hello"))
                .map(payload -> StandardCharsets.UTF_8.decode(payload.getData()).toString())
                .doOnNext(System.out::println)
                .take(10)
                .thenEmpty(socket.close())
                .block();

        // if both client & server sockets are closed, then unblocks quickly
        // otherwise it will cause timeout
        stats.onAllSocketsClosed.block();

        assertThat("Number of opened socket should be 2", stats.createdSocketCount, is(2));
        assertThat("Number of created duplex connections should be 2", stats.createdDuplexConnectionCount, is(2));
        assertThat("Number of closed duplex connections should be 2", stats.closedDuplexConnectionCount, is(2));
        assertThat("Number of closed socket should be 2", stats.closedSocketCount, is(2));
        assertThat("Number of disconnected connections should be 0", stats.disconnectedSocketCount, is(0));
        assertThat("Number of read frames mismatches with the expectation", stats.currentReadFrameIndex, is(stats.expectedFrameReads.size()));
        assertThat("Number of written frames mismatches with the expectation", stats.currentWriteFrameIndex, is(stats.expectedFrameWrites.size()));
    }

    private static class SocketAcceptorImpl implements SocketAcceptor {
        @Override
        public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket reactiveSocket) {
            return Mono.just(new AbstractRSocket() {
                @Override
                public Flux<Payload> requestStream(Payload payload) {
                    return Flux.just(new PayloadImpl("RESPONSE", "METADATA"));
                }
            });
        }
    }
}
