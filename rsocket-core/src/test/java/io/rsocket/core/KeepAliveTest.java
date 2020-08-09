/// *
// * Copyright 2015-2019 the original author or authors.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package io.rsocket.core;
//
// import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
// import static io.rsocket.keepalive.KeepAliveHandler.DefaultKeepAliveHandler;
// import static io.rsocket.keepalive.KeepAliveHandler.ResumableKeepAliveHandler;
//
// import io.netty.buffer.ByteBuf;
// import io.netty.buffer.ByteBufAllocator;
// import io.netty.buffer.Unpooled;
// import io.rsocket.RSocket;
// import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
// import io.rsocket.exceptions.ConnectionErrorException;
// import io.rsocket.frame.FrameHeaderCodec;
// import io.rsocket.frame.FrameType;
// import io.rsocket.frame.KeepAliveFrameCodec;
// import io.rsocket.lease.RequesterLeaseHandler;
// import io.rsocket.resume.InMemoryResumableFramesStore;
//// import io.rsocket.resume.ResumableDuplexConnection;
// import io.rsocket.test.util.TestDuplexConnection;
// import io.rsocket.util.DefaultPayload;
// import java.time.Duration;
// import org.assertj.core.api.Assertions;
// import org.junit.jupiter.api.AfterEach;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
// import reactor.core.Disposable;
// import reactor.core.publisher.Flux;
// import reactor.core.publisher.Mono;
// import reactor.test.StepVerifier;
// import reactor.test.scheduler.VirtualTimeScheduler;
//
// public class KeepAliveTest {
//  private static final int KEEP_ALIVE_INTERVAL = 100;
//  private static final int KEEP_ALIVE_TIMEOUT = 1000;
//  private static final int RESUMABLE_KEEP_ALIVE_TIMEOUT = 200;
//
//  VirtualTimeScheduler virtualTimeScheduler;
//
//  @BeforeEach
//  public void setUp() {
//    virtualTimeScheduler = VirtualTimeScheduler.getOrSet();
//  }
//
//  @AfterEach
//  public void tearDown() {
//    VirtualTimeScheduler.reset();
//  }
//
//  static RSocketState requester(int tickPeriod, int timeout) {
//    LeaksTrackingByteBufAllocator allocator =
//        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
//    TestDuplexConnection connection = new TestDuplexConnection(allocator);
//    RSocketRequester rSocket =
//        new RSocketRequester(
//            connection,
//            DefaultPayload::create,
//            StreamIdSupplier.clientSupplier(),
//            0,
//            FRAME_LENGTH_MASK,
//            Integer.MAX_VALUE,
//            tickPeriod,
//            timeout,
//            new DefaultKeepAliveHandler(connection),
//            RequesterLeaseHandler.None);
//    return new RSocketState(rSocket, allocator, connection);
//  }
//
//  static ResumableRSocketState resumableRequester(int tickPeriod, int timeout) {
//    LeaksTrackingByteBufAllocator allocator =
//        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
//    TestDuplexConnection connection = new TestDuplexConnection(allocator);
////    ResumableDuplexConnection resumableConnection =
////        new ResumableDuplexConnection(
////            "test",
////            connection,
////            new InMemoryResumableFramesStore("test", 10_000),
////            Duration.ofSeconds(10),
////            false);
//
//    RSocketRequester rSocket =
//        new RSocketRequester(
//            resumableConnection,
//            DefaultPayload::create,
//            StreamIdSupplier.clientSupplier(),
//            0,
//            FRAME_LENGTH_MASK,
//            Integer.MAX_VALUE,
//            tickPeriod,
//            timeout,
//            new ResumableKeepAliveHandler(resumableConnection),
//            RequesterLeaseHandler.None);
//    return new ResumableRSocketState(rSocket, connection, resumableConnection, allocator);
//  }
//
//  @Test
//  void rSocketNotDisposedOnPresentKeepAlives() {
//    RSocketState requesterState = requester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
//
//    TestDuplexConnection connection = requesterState.connection();
//
//    Disposable disposable =
//        Flux.interval(Duration.ofMillis(KEEP_ALIVE_INTERVAL))
//            .subscribe(
//                n ->
//                    connection.addToReceivedBuffer(
//                        KeepAliveFrameCodec.encode(
//                            requesterState.allocator, true, 0, Unpooled.EMPTY_BUFFER)));
//
//    virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_TIMEOUT * 2));
//
//    RSocket rSocket = requesterState.rSocket();
//
//    Assertions.assertThat(rSocket.isDisposed()).isFalse();
//
//    disposable.dispose();
//
//    requesterState.connection.dispose();
//    requesterState.rSocket.dispose();
//
//    Assertions.assertThat(requesterState.connection.getSent()).allMatch(ByteBuf::release);
//
//    requesterState.allocator.assertHasNoLeaks();
//  }
//
//  @Test
//  void noKeepAlivesSentAfterRSocketDispose() {
//    RSocketState requesterState = requester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
//
//    requesterState.rSocket().dispose();
//
//    Duration duration = Duration.ofMillis(500);
//
// StepVerifier.create(Flux.from(requesterState.connection().getSentAsPublisher()).take(duration))
//        .then(() -> virtualTimeScheduler.advanceTimeBy(duration))
//        .expectComplete()
//        .verify(Duration.ofSeconds(1));
//
//    requesterState.allocator.assertHasNoLeaks();
//  }
//
//  @Test
//  void rSocketDisposedOnMissingKeepAlives() {
//    RSocketState requesterState = requester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
//
//    RSocket rSocket = requesterState.rSocket();
//
//    virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_TIMEOUT * 2));
//
//    Assertions.assertThat(rSocket.isDisposed()).isTrue();
//    rSocket
//        .onClose()
//        .as(StepVerifier::create)
//        .expectError(ConnectionErrorException.class)
//        .verify(Duration.ofMillis(100));
//
//    Assertions.assertThat(requesterState.connection.getSent()).allMatch(ByteBuf::release);
//
//    requesterState.allocator.assertHasNoLeaks();
//  }
//
//  @Test
//  void clientRequesterSendsKeepAlives() {
//    RSocketState RSocketState = requester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
//    TestDuplexConnection connection = RSocketState.connection();
//
//    StepVerifier.create(Flux.from(connection.getSentAsPublisher()).take(3))
//        .then(() -> virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_INTERVAL)))
//        .expectNextMatches(this::keepAliveFrameWithRespondFlag)
//        .then(() -> virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_INTERVAL)))
//        .expectNextMatches(this::keepAliveFrameWithRespondFlag)
//        .then(() -> virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_INTERVAL)))
//        .expectNextMatches(this::keepAliveFrameWithRespondFlag)
//        .expectComplete()
//        .verify(Duration.ofSeconds(5));
//
//    RSocketState.rSocket.dispose();
//    RSocketState.connection.dispose();
//
//    RSocketState.allocator.assertHasNoLeaks();
//  }
//
//  @Test
//  void requesterRespondsToKeepAlives() {
//    RSocketState rSocketState = requester(100_000, 100_000);
//    TestDuplexConnection connection = rSocketState.connection();
//    Duration duration = Duration.ofMillis(100);
//    Mono.delay(duration)
//        .subscribe(
//            l ->
//                connection.addToReceivedBuffer(
//                    KeepAliveFrameCodec.encode(
//                        rSocketState.allocator, true, 0, Unpooled.EMPTY_BUFFER)));
//
//    StepVerifier.create(Flux.from(connection.getSentAsPublisher()).take(1))
//        .then(() -> virtualTimeScheduler.advanceTimeBy(duration))
//        .expectNextMatches(this::keepAliveFrameWithoutRespondFlag)
//        .expectComplete()
//        .verify(Duration.ofSeconds(5));
//
//    rSocketState.rSocket.dispose();
//    rSocketState.connection.dispose();
//
//    rSocketState.allocator.assertHasNoLeaks();
//  }
//
//  @Test
//  void resumableRequesterNoKeepAlivesAfterDisconnect() {
//    ResumableRSocketState rSocketState =
//        resumableRequester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
//    TestDuplexConnection testConnection = rSocketState.connection();
//    ResumableDuplexConnection resumableDuplexConnection =
// rSocketState.resumableDuplexConnection();
//
//    resumableDuplexConnection.disconnect();
//
//    Duration duration = Duration.ofMillis(500);
//    StepVerifier.create(Flux.from(testConnection.getSentAsPublisher()).take(duration))
//        .then(() -> virtualTimeScheduler.advanceTimeBy(duration))
//        .expectComplete()
//        .verify(Duration.ofSeconds(5));
//
//    rSocketState.rSocket.dispose();
//    rSocketState.connection.dispose();
//
//    rSocketState.allocator.assertHasNoLeaks();
//  }
//
//  @Test
//  void resumableRequesterKeepAlivesAfterReconnect() {
//    ResumableRSocketState rSocketState =
//        resumableRequester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
//    ResumableDuplexConnection resumableDuplexConnection =
// rSocketState.resumableDuplexConnection();
//    resumableDuplexConnection.disconnect();
//    TestDuplexConnection newTestConnection = new TestDuplexConnection(rSocketState.alloc());
//    resumableDuplexConnection.reconnect(newTestConnection);
//    resumableDuplexConnection.resume(0, 0, ignored -> Mono.empty());
//
//    StepVerifier.create(Flux.from(newTestConnection.getSentAsPublisher()).take(1))
//        .then(() -> virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_INTERVAL)))
//        .expectNextMatches(frame -> keepAliveFrame(frame) && frame.release())
//        .expectComplete()
//        .verify(Duration.ofSeconds(5));
//
//    rSocketState.rSocket.dispose();
//    rSocketState.connection.dispose();
//
//    rSocketState.allocator.assertHasNoLeaks();
//  }
//
//  @Test
//  void resumableRequesterNoKeepAlivesAfterDispose() {
//    ResumableRSocketState rSocketState =
//        resumableRequester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
//    rSocketState.rSocket().dispose();
//    Duration duration = Duration.ofMillis(500);
//    StepVerifier.create(Flux.from(rSocketState.connection().getSentAsPublisher()).take(duration))
//        .then(() -> virtualTimeScheduler.advanceTimeBy(duration))
//        .expectComplete()
//        .verify(Duration.ofSeconds(5));
//
//    rSocketState.rSocket.dispose();
//    rSocketState.connection.dispose();
//
//    rSocketState.allocator.assertHasNoLeaks();
//  }
//
//  @Test
//  void resumableRSocketsNotDisposedOnMissingKeepAlives() throws InterruptedException {
//    ResumableRSocketState resumableRequesterState =
//        resumableRequester(KEEP_ALIVE_INTERVAL, RESUMABLE_KEEP_ALIVE_TIMEOUT);
//    RSocket rSocket = resumableRequesterState.rSocket();
//    TestDuplexConnection connection = resumableRequesterState.connection();
//
//    virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(500));
//
//    Assertions.assertThat(rSocket.isDisposed()).isFalse();
//    Assertions.assertThat(connection.isDisposed()).isTrue();
//
//
// Assertions.assertThat(resumableRequesterState.connection.getSent()).allMatch(ByteBuf::release);
//
//    resumableRequesterState.connection.dispose();
//    resumableRequesterState.rSocket.dispose();
//
//    resumableRequesterState.allocator.assertHasNoLeaks();
//  }
//
//  private boolean keepAliveFrame(ByteBuf frame) {
//    return FrameHeaderCodec.frameType(frame) == FrameType.KEEPALIVE;
//  }
//
//  private boolean keepAliveFrameWithRespondFlag(ByteBuf frame) {
//    return keepAliveFrame(frame) && KeepAliveFrameCodec.respondFlag(frame) && frame.release();
//  }
//
//  private boolean keepAliveFrameWithoutRespondFlag(ByteBuf frame) {
//    return keepAliveFrame(frame) && !KeepAliveFrameCodec.respondFlag(frame) && frame.release();
//  }
//
//  static class RSocketState {
//    private final RSocket rSocket;
//    private final TestDuplexConnection connection;
//    private final LeaksTrackingByteBufAllocator allocator;
//
//    public RSocketState(
//        RSocket rSocket, LeaksTrackingByteBufAllocator allocator, TestDuplexConnection connection)
// {
//      this.rSocket = rSocket;
//      this.connection = connection;
//      this.allocator = allocator;
//    }
//
//    public TestDuplexConnection connection() {
//      return connection;
//    }
//
//    public RSocket rSocket() {
//      return rSocket;
//    }
//
//    public LeaksTrackingByteBufAllocator alloc() {
//      return allocator;
//    }
//  }
//
//  static class ResumableRSocketState {
//    private final RSocket rSocket;
//    private final TestDuplexConnection connection;
//    private final ResumableDuplexConnection resumableDuplexConnection;
//    private final LeaksTrackingByteBufAllocator allocator;
//
//    public ResumableRSocketState(
//        RSocket rSocket,
//        TestDuplexConnection connection,
//        ResumableDuplexConnection resumableDuplexConnection,
//        LeaksTrackingByteBufAllocator allocator) {
//      this.rSocket = rSocket;
//      this.connection = connection;
//      this.resumableDuplexConnection = resumableDuplexConnection;
//      this.allocator = allocator;
//    }
//
//    public TestDuplexConnection connection() {
//      return connection;
//    }
//
//    public ResumableDuplexConnection resumableDuplexConnection() {
//      return resumableDuplexConnection;
//    }
//
//    public RSocket rSocket() {
//      return rSocket;
//    }
//
//    public LeaksTrackingByteBufAllocator alloc() {
//      return allocator;
//    }
//  }
// }
