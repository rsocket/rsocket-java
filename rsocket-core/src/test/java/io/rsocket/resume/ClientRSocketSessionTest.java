package io.rsocket.resume;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import io.rsocket.FrameAssert;
import io.rsocket.exceptions.ConnectionCloseException;
import io.rsocket.exceptions.RejectedResumeException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameCodec;
import io.rsocket.frame.ResumeOkFrameCodec;
import io.rsocket.keepalive.KeepAliveSupport;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.test.util.TestDuplexConnection;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Operators;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

public class ClientRSocketSessionTest {

  @Test
  void sessionTimeoutSmokeTest() {
    final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.getOrSet();
    try {
      final TestClientTransport transport = new TestClientTransport();
      final InMemoryResumableFramesStore framesStore =
          new InMemoryResumableFramesStore("test", Unpooled.EMPTY_BUFFER, 100);

      transport.connect().subscribe();

      final ResumableDuplexConnection resumableDuplexConnection =
          new ResumableDuplexConnection(
              "test", Unpooled.EMPTY_BUFFER, transport.testConnection(), framesStore);

      resumableDuplexConnection.receive().subscribe();

      final ClientRSocketSession session =
          new ClientRSocketSession(
              Unpooled.EMPTY_BUFFER,
              resumableDuplexConnection,
              transport.connect().delaySubscription(Duration.ofMillis(1)),
              c -> {
                AtomicBoolean firstHandled = new AtomicBoolean();
                return ((TestDuplexConnection) c)
                    .receive()
                    .next()
                    .doOnNext(__ -> firstHandled.set(true))
                    .doOnCancel(
                        () -> {
                          if (firstHandled.compareAndSet(false, true)) {
                            c.dispose();
                          }
                        })
                    .map(b -> Tuples.of(b, c));
              },
              framesStore,
              Duration.ofMinutes(1),
              Retry.indefinitely(),
              true);

      final KeepAliveSupport.ClientKeepAliveSupport keepAliveSupport =
          new KeepAliveSupport.ClientKeepAliveSupport(transport.alloc(), 1000000, 10000000);
      session.setKeepAliveSupport(keepAliveSupport);

      // connection is active. just advance time
      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(10));
      assertThat(session.s).isNull();
      assertThat(session.isDisposed()).isFalse();

      // deactivate connection
      transport.testConnection().dispose();
      assertThat(transport.testConnection().isDisposed()).isTrue();
      // ensures timeout has been started
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();

      // advance time so new connection is received
      virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(1));
      assertThat(transport.testConnection().isDisposed()).isFalse();
      // timeout should be still active since no RESUME_Ok frame has been received yet
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();

      // advance time
      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(50));
      // timeout should not terminate current connection
      assertThat(transport.testConnection().isDisposed()).isFalse();

      // send RESUME_OK frame
      transport
          .testConnection()
          .addToReceivedBuffer(ResumeOkFrameCodec.encode(transport.alloc(), 0));
      assertThat(transport.testConnection().isDisposed()).isFalse();
      // timeout should be terminated
      assertThat(session.s).isNull();
      assertThat(session.isDisposed()).isFalse();
      FrameAssert.assertThat(transport.testConnection().pollFrame())
          .hasStreamIdZero()
          .typeOf(FrameType.RESUME)
          .matches(ReferenceCounted::release);

      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(15));

      // disconnects for the second time
      transport.testConnection().dispose();
      assertThat(transport.testConnection().isDisposed()).isTrue();
      // ensures timeout has been started
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();

      // advance time so new connection is received
      virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(1));
      assertThat(transport.testConnection().isDisposed()).isFalse();
      // timeout should be still active since no RESUME_Ok frame has been received yet
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();
      FrameAssert.assertThat(transport.testConnection().pollFrame())
          .hasStreamIdZero()
          .typeOf(FrameType.RESUME)
          .matches(ReferenceCounted::release);

      transport
          .testConnection()
          .addToReceivedBuffer(
              ErrorFrameCodec.encode(
                  transport.alloc(), 0, new ConnectionCloseException("some message")));
      // connection should be closed because of the wrong first frame
      assertThat(transport.testConnection().isDisposed()).isTrue();
      // ensures timeout is still in progress
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();

      // advance time
      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(30));
      // should obtain new connection
      assertThat(transport.testConnection().isDisposed()).isFalse();
      // timeout should be still active since no RESUME_OK frame has been received yet
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();

      FrameAssert.assertThat(transport.testConnection().pollFrame())
          .hasStreamIdZero()
          .typeOf(FrameType.RESUME)
          .matches(ReferenceCounted::release);

      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(30));

      assertThat(session.s).isEqualTo(Operators.cancelledSubscription());
      assertThat(transport.testConnection().isDisposed()).isTrue();

      assertThat(session.isDisposed()).isTrue();

      resumableDuplexConnection.onClose().as(StepVerifier::create).expectComplete().verify();
      keepAliveSupport.dispose();
      transport.alloc().assertHasNoLeaks();
    } finally {
      VirtualTimeScheduler.reset();
    }
  }

  @Test
  void sessionTerminationOnWrongFrameTest() {
    final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.getOrSet();
    try {

      final TestClientTransport transport = new TestClientTransport();
      final InMemoryResumableFramesStore framesStore =
          new InMemoryResumableFramesStore("test", Unpooled.EMPTY_BUFFER, 100);

      transport.connect().subscribe();

      final ResumableDuplexConnection resumableDuplexConnection =
          new ResumableDuplexConnection(
              "test", Unpooled.EMPTY_BUFFER, transport.testConnection(), framesStore);

      resumableDuplexConnection.receive().subscribe();

      final ClientRSocketSession session =
          new ClientRSocketSession(
              Unpooled.EMPTY_BUFFER,
              resumableDuplexConnection,
              transport.connect().delaySubscription(Duration.ofMillis(1)),
              c -> {
                AtomicBoolean firstHandled = new AtomicBoolean();
                return ((TestDuplexConnection) c)
                    .receive()
                    .next()
                    .doOnNext(__ -> firstHandled.set(true))
                    .doOnCancel(
                        () -> {
                          if (firstHandled.compareAndSet(false, true)) {
                            c.dispose();
                          }
                        })
                    .map(b -> Tuples.of(b, c));
              },
              framesStore,
              Duration.ofMinutes(1),
              Retry.indefinitely(),
              true);

      final KeepAliveSupport.ClientKeepAliveSupport keepAliveSupport =
          new KeepAliveSupport.ClientKeepAliveSupport(transport.alloc(), 1000000, 10000000);
      session.setKeepAliveSupport(keepAliveSupport);

      // connection is active. just advance time
      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(10));
      assertThat(session.s).isNull();
      assertThat(session.isDisposed()).isFalse();

      // deactivate connection
      transport.testConnection().dispose();
      assertThat(transport.testConnection().isDisposed()).isTrue();
      // ensures timeout has been started
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();

      // advance time so new connection is received
      virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(1));
      assertThat(transport.testConnection().isDisposed()).isFalse();
      // timeout should be still active since no RESUME_Ok frame has been received yet
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();

      FrameAssert.assertThat(transport.testConnection().pollFrame())
          .hasStreamIdZero()
          .typeOf(FrameType.RESUME)
          .matches(ReferenceCounted::release);

      // advance time
      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(50));
      // timeout should not terminate current connection
      assertThat(transport.testConnection().isDisposed()).isFalse();

      // send RESUME_OK frame
      transport
          .testConnection()
          .addToReceivedBuffer(ResumeOkFrameCodec.encode(transport.alloc(), 0));
      assertThat(transport.testConnection().isDisposed()).isFalse();
      // timeout should be terminated
      assertThat(session.s).isNull();
      assertThat(session.isDisposed()).isFalse();

      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(15));

      // disconnects for the second time
      transport.testConnection().dispose();
      assertThat(transport.testConnection().isDisposed()).isTrue();
      // ensures timeout has been started
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();

      // advance time so new connection is received
      virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(1));
      assertThat(transport.testConnection().isDisposed()).isFalse();
      // timeout should be still active since no RESUME_Ok frame has been received yet
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();

      FrameAssert.assertThat(transport.testConnection().pollFrame())
          .hasStreamIdZero()
          .typeOf(FrameType.RESUME)
          .matches(ReferenceCounted::release);

      // Send KEEPALIVE frame as a first frame
      transport
          .testConnection()
          .addToReceivedBuffer(
              KeepAliveFrameCodec.encode(transport.alloc(), false, 0, Unpooled.EMPTY_BUFFER));

      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(30));

      assertThat(session.s).isEqualTo(Operators.cancelledSubscription());
      assertThat(transport.testConnection().isDisposed()).isTrue();
      assertThat(session.isDisposed()).isTrue();

      FrameAssert.assertThat(transport.testConnection().pollFrame())
          .hasStreamIdZero()
          .typeOf(FrameType.ERROR)
          .matches(ReferenceCounted::release);

      resumableDuplexConnection
          .onClose()
          .as(StepVerifier::create)
          .expectErrorMessage("RESUME_OK frame must be received before any others")
          .verify();
      keepAliveSupport.dispose();
      transport.alloc().assertHasNoLeaks();
    } finally {
      VirtualTimeScheduler.reset();
    }
  }

  @Test
  void shouldErrorWithNoRetriesOnErrorFrameTest() {
    final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.getOrSet();
    try {
      final TestClientTransport transport = new TestClientTransport();
      final InMemoryResumableFramesStore framesStore =
          new InMemoryResumableFramesStore("test", Unpooled.EMPTY_BUFFER, 100);

      transport.connect().subscribe();

      final ResumableDuplexConnection resumableDuplexConnection =
          new ResumableDuplexConnection(
              "test", Unpooled.EMPTY_BUFFER, transport.testConnection(), framesStore);

      resumableDuplexConnection.receive().subscribe();

      final ClientRSocketSession session =
          new ClientRSocketSession(
              Unpooled.EMPTY_BUFFER,
              resumableDuplexConnection,
              transport.connect().delaySubscription(Duration.ofMillis(1)),
              c -> {
                AtomicBoolean firstHandled = new AtomicBoolean();
                return ((TestDuplexConnection) c)
                    .receive()
                    .next()
                    .doOnNext(__ -> firstHandled.set(true))
                    .doOnCancel(
                        () -> {
                          if (firstHandled.compareAndSet(false, true)) {
                            c.dispose();
                          }
                        })
                    .map(b -> Tuples.of(b, c));
              },
              framesStore,
              Duration.ofMinutes(1),
              Retry.indefinitely(),
              true);

      final KeepAliveSupport.ClientKeepAliveSupport keepAliveSupport =
          new KeepAliveSupport.ClientKeepAliveSupport(transport.alloc(), 1000000, 10000000);
      session.setKeepAliveSupport(keepAliveSupport);

      // connection is active. just advance time
      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(10));
      assertThat(session.s).isNull();
      assertThat(session.isDisposed()).isFalse();

      // deactivate connection
      transport.testConnection().dispose();
      assertThat(transport.testConnection().isDisposed()).isTrue();
      // ensures timeout has been started
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();

      // advance time so new connection is received
      virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(1));
      assertThat(transport.testConnection().isDisposed()).isFalse();
      // timeout should be still active since no RESUME_Ok frame has been received yet
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isFalse();

      FrameAssert.assertThat(transport.testConnection().pollFrame())
          .hasStreamIdZero()
          .typeOf(FrameType.RESUME)
          .matches(ReferenceCounted::release);

      // advance time
      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(50));
      // timeout should not terminate current connection
      assertThat(transport.testConnection().isDisposed()).isFalse();

      // send REJECTED_RESUME_ERROR frame
      transport
          .testConnection()
          .addToReceivedBuffer(
              ErrorFrameCodec.encode(
                  transport.alloc(), 0, new RejectedResumeException("failed resumption")));
      assertThat(transport.testConnection().isDisposed()).isTrue();
      // timeout should be terminated
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isTrue();

      resumableDuplexConnection
          .onClose()
          .as(StepVerifier::create)
          .expectError(RejectedResumeException.class)
          .verify();
      keepAliveSupport.dispose();
      transport.alloc().assertHasNoLeaks();
    } finally {
      VirtualTimeScheduler.reset();
    }
  }

  @Test
  void shouldTerminateConnectionOnIllegalStateInKeepAliveFrame() {
    final VirtualTimeScheduler virtualTimeScheduler = VirtualTimeScheduler.getOrSet();
    try {
      final TestClientTransport transport = new TestClientTransport();
      final InMemoryResumableFramesStore framesStore =
          new InMemoryResumableFramesStore("test", Unpooled.EMPTY_BUFFER, 100);

      transport.connect().subscribe();

      final ResumableDuplexConnection resumableDuplexConnection =
          new ResumableDuplexConnection(
              "test", Unpooled.EMPTY_BUFFER, transport.testConnection(), framesStore);

      resumableDuplexConnection.receive().subscribe();

      final ClientRSocketSession session =
          new ClientRSocketSession(
              Unpooled.EMPTY_BUFFER,
              resumableDuplexConnection,
              transport.connect().delaySubscription(Duration.ofMillis(1)),
              c -> {
                AtomicBoolean firstHandled = new AtomicBoolean();
                return ((TestDuplexConnection) c)
                    .receive()
                    .next()
                    .doOnNext(__ -> firstHandled.set(true))
                    .doOnCancel(
                        () -> {
                          if (firstHandled.compareAndSet(false, true)) {
                            c.dispose();
                          }
                        })
                    .map(b -> Tuples.of(b, c));
              },
              framesStore,
              Duration.ofMinutes(1),
              Retry.indefinitely(),
              true);

      final KeepAliveSupport.ClientKeepAliveSupport keepAliveSupport =
          new KeepAliveSupport.ClientKeepAliveSupport(transport.alloc(), 1000000, 10000000);
      keepAliveSupport.resumeState(session);
      session.setKeepAliveSupport(keepAliveSupport);

      // connection is active. just advance time
      virtualTimeScheduler.advanceTimeBy(Duration.ofSeconds(10));
      assertThat(session.s).isNull();
      assertThat(session.isDisposed()).isFalse();

      final ByteBuf keepAliveFrame =
          KeepAliveFrameCodec.encode(transport.alloc(), false, 1529, Unpooled.EMPTY_BUFFER);
      keepAliveSupport.receive(keepAliveFrame);
      keepAliveFrame.release();

      assertThat(transport.testConnection().isDisposed()).isTrue();
      // timeout should be terminated
      assertThat(session.s).isNotNull();
      assertThat(session.isDisposed()).isTrue();

      FrameAssert.assertThat(transport.testConnection().pollFrame())
          .hasStreamIdZero()
          .typeOf(FrameType.ERROR)
          .matches(ReferenceCounted::release);

      resumableDuplexConnection.onClose().as(StepVerifier::create).expectError().verify();
      keepAliveSupport.dispose();
      transport.alloc().assertHasNoLeaks();
    } finally {
      VirtualTimeScheduler.reset();
    }
  }
}
