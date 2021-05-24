package io.rsocket.resume;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import io.rsocket.RaceTestConstants;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.internal.subscriber.AssertSubscriber;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Disposable;
import reactor.core.publisher.Hooks;
import reactor.test.util.RaceTestUtils;

public class InMemoryResumeStoreTest {

  @Test
  void saveNonResumableFrame() {
    final InMemoryResumableFramesStore store = inMemoryStore(25);
    final UnboundedProcessor sender = new UnboundedProcessor();

    store.saveFrames(sender).subscribe();

    final AssertSubscriber<ByteBuf> assertSubscriber =
        store.resumeStream().subscribeWith(AssertSubscriber.create());

    final ByteBuf frame1 = fakeConnectionFrame(10);
    final ByteBuf frame2 = fakeConnectionFrame(35);

    sender.onNext(frame1);
    sender.onNext(frame2);

    assertThat(store.cachedFrames.size()).isZero();
    assertThat(store.cacheSize).isZero();
    assertThat(store.firstAvailableFramePosition).isZero();

    assertSubscriber.assertValueCount(2).values().forEach(ByteBuf::release);

    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isZero();
  }

  @Test
  void saveWithoutTailRemoval() {
    final InMemoryResumableFramesStore store = inMemoryStore(25);
    final UnboundedProcessor sender = new UnboundedProcessor();

    store.saveFrames(sender).subscribe();

    final AssertSubscriber<ByteBuf> assertSubscriber =
        store.resumeStream().subscribeWith(AssertSubscriber.create());

    final ByteBuf frame = fakeResumableFrame(10);

    sender.onNext(frame);

    assertThat(store.cachedFrames.size()).isEqualTo(1);
    assertThat(store.cacheSize).isEqualTo(frame.readableBytes());
    assertThat(store.firstAvailableFramePosition).isZero();

    assertSubscriber.assertValueCount(1).values().forEach(ByteBuf::release);

    assertThat(frame.refCnt()).isOne();
  }

  @Test
  void saveRemoveOneFromTail() {
    final InMemoryResumableFramesStore store = inMemoryStore(25);
    final UnboundedProcessor sender = new UnboundedProcessor();

    store.saveFrames(sender).subscribe();

    final AssertSubscriber<ByteBuf> assertSubscriber =
        store.resumeStream().subscribeWith(AssertSubscriber.create());
    final ByteBuf frame1 = fakeResumableFrame(20);
    final ByteBuf frame2 = fakeResumableFrame(10);

    sender.onNext(frame1);
    sender.onNext(frame2);

    assertThat(store.cachedFrames.size()).isOne();
    assertThat(store.cacheSize).isEqualTo(frame2.readableBytes());
    assertThat(store.firstAvailableFramePosition).isEqualTo(frame1.readableBytes());

    assertSubscriber.assertValueCount(2).values().forEach(ByteBuf::release);

    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isOne();
  }

  @Test
  void saveRemoveTwoFromTail() {
    final InMemoryResumableFramesStore store = inMemoryStore(25);
    final UnboundedProcessor sender = new UnboundedProcessor();

    store.saveFrames(sender).subscribe();

    final AssertSubscriber<ByteBuf> assertSubscriber =
        store.resumeStream().subscribeWith(AssertSubscriber.create());

    final ByteBuf frame1 = fakeResumableFrame(10);
    final ByteBuf frame2 = fakeResumableFrame(10);
    final ByteBuf frame3 = fakeResumableFrame(20);

    sender.onNext(frame1);
    sender.onNext(frame2);
    sender.onNext(frame3);

    assertThat(store.cachedFrames.size()).isOne();
    assertThat(store.cacheSize).isEqualTo(frame3.readableBytes());
    assertThat(store.firstAvailableFramePosition).isEqualTo(size(frame1, frame2));

    assertSubscriber.assertValueCount(3).values().forEach(ByteBuf::release);

    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isZero();
    assertThat(frame3.refCnt()).isOne();
  }

  @Test
  void saveBiggerThanStore() {
    final InMemoryResumableFramesStore store = inMemoryStore(25);
    final UnboundedProcessor sender = new UnboundedProcessor();

    store.saveFrames(sender).subscribe();

    final AssertSubscriber<ByteBuf> assertSubscriber =
        store.resumeStream().subscribeWith(AssertSubscriber.create());
    final ByteBuf frame1 = fakeResumableFrame(10);
    final ByteBuf frame2 = fakeResumableFrame(10);
    final ByteBuf frame3 = fakeResumableFrame(30);

    sender.onNext(frame1);
    sender.onNext(frame2);
    sender.onNext(frame3);

    assertThat(store.cachedFrames.size()).isZero();
    assertThat(store.cacheSize).isZero();
    assertThat(store.firstAvailableFramePosition).isEqualTo(size(frame1, frame2, frame3));

    assertSubscriber.assertValueCount(3).values().forEach(ByteBuf::release);

    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isZero();
    assertThat(frame3.refCnt()).isZero();
  }

  @Test
  void releaseFrames() {
    final InMemoryResumableFramesStore store = inMemoryStore(100);

    final UnboundedProcessor producer = new UnboundedProcessor();
    store.saveFrames(producer).subscribe();

    final AssertSubscriber<ByteBuf> assertSubscriber =
        store.resumeStream().subscribeWith(AssertSubscriber.create());

    final ByteBuf frame1 = fakeResumableFrame(10);
    final ByteBuf frame2 = fakeResumableFrame(10);
    final ByteBuf frame3 = fakeResumableFrame(30);

    producer.onNext(frame1);
    producer.onNext(frame2);
    producer.onNext(frame3);

    store.releaseFrames(20);

    assertThat(store.cachedFrames.size()).isOne();
    assertThat(store.cacheSize).isEqualTo(frame3.readableBytes());
    assertThat(store.firstAvailableFramePosition).isEqualTo(size(frame1, frame2));

    assertSubscriber.assertValueCount(3).values().forEach(ByteBuf::release);

    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isZero();
    assertThat(frame3.refCnt()).isOne();
  }

  @Test
  void receiveImpliedPosition() {
    final InMemoryResumableFramesStore store = inMemoryStore(100);

    ByteBuf frame1 = fakeResumableFrame(10);
    ByteBuf frame2 = fakeResumableFrame(30);

    store.resumableFrameReceived(frame1);
    store.resumableFrameReceived(frame2);

    assertThat(store.frameImpliedPosition()).isEqualTo(size(frame1, frame2));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void ensuresCleansOnTerminal(boolean hasSubscriber) {
    final InMemoryResumableFramesStore store = inMemoryStore(100);

    final UnboundedProcessor producer = new UnboundedProcessor();
    store.saveFrames(producer).subscribe();

    final AssertSubscriber<ByteBuf> assertSubscriber =
        hasSubscriber ? store.resumeStream().subscribeWith(AssertSubscriber.create()) : null;

    final ByteBuf frame1 = fakeResumableFrame(10);
    final ByteBuf frame2 = fakeResumableFrame(10);
    final ByteBuf frame3 = fakeResumableFrame(30);

    producer.onNext(frame1);
    producer.onNext(frame2);
    producer.onNext(frame3);
    producer.onComplete();

    assertThat(store.cachedFrames.size()).isZero();
    assertThat(store.cacheSize).isZero();

    assertThat(producer.isDisposed()).isTrue();

    if (hasSubscriber) {
      assertSubscriber.assertValueCount(3).assertTerminated().values().forEach(ByteBuf::release);
    }

    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isZero();
    assertThat(frame3.refCnt()).isZero();
  }

  @Test
  void ensuresCleansOnTerminalLateSubscriber() {
    final InMemoryResumableFramesStore store = inMemoryStore(100);

    final UnboundedProcessor producer = new UnboundedProcessor();
    store.saveFrames(producer).subscribe();

    final ByteBuf frame1 = fakeResumableFrame(10);
    final ByteBuf frame2 = fakeResumableFrame(10);
    final ByteBuf frame3 = fakeResumableFrame(30);

    producer.onNext(frame1);
    producer.onNext(frame2);
    producer.onNext(frame3);
    producer.onComplete();

    assertThat(store.cachedFrames.size()).isZero();
    assertThat(store.cacheSize).isZero();

    assertThat(producer.isDisposed()).isTrue();

    final AssertSubscriber<ByteBuf> assertSubscriber =
        store.resumeStream().subscribeWith(AssertSubscriber.create());
    assertSubscriber.assertTerminated();

    assertThat(frame1.refCnt()).isZero();
    assertThat(frame2.refCnt()).isZero();
    assertThat(frame3.refCnt()).isZero();
  }

  @ParameterizedTest(name = "Sending vs Reconnect Race Test. WithLateSubscriber[{0}]")
  @ValueSource(booleans = {true, false})
  void sendingVsReconnectRaceTest(boolean withLateSubscriber) {
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final InMemoryResumableFramesStore store = inMemoryStore(Integer.MAX_VALUE);
      final UnboundedProcessor frames = new UnboundedProcessor();
      final BlockingQueue<ByteBuf> receivedFrames = new ArrayBlockingQueue<>(10);
      final AtomicInteger receivedPosition = new AtomicInteger();

      store.saveFrames(frames).subscribe();

      final Consumer<ByteBuf> consumer =
          f -> {
            if (ResumableDuplexConnection.isResumableFrame(f)) {
              receivedPosition.addAndGet(f.readableBytes());
              receivedFrames.offer(f);
              return;
            }
            f.release();
          };
      final AtomicReference<Disposable> disposableReference =
          new AtomicReference<>(
              withLateSubscriber ? null : store.resumeStream().subscribe(consumer));

      final ByteBuf byteBuf1 = fakeResumableFrame(5);
      final ByteBuf byteBuf11 = fakeConnectionFrame(5);
      final ByteBuf byteBuf2 = fakeResumableFrame(6);
      final ByteBuf byteBuf21 = fakeConnectionFrame(5);
      final ByteBuf byteBuf3 = fakeResumableFrame(7);
      final ByteBuf byteBuf31 = fakeConnectionFrame(5);
      final ByteBuf byteBuf4 = fakeResumableFrame(8);
      final ByteBuf byteBuf41 = fakeConnectionFrame(5);
      final ByteBuf byteBuf5 = fakeResumableFrame(25);
      final ByteBuf byteBuf51 = fakeConnectionFrame(35);

      RaceTestUtils.race(
          () -> {
            if (withLateSubscriber) {
              disposableReference.set(store.resumeStream().subscribe(consumer));
            }

            // disconnect
            disposableReference.get().dispose();

            while (InMemoryResumableFramesStore.isWorkInProgress(store.state)) {
              // ignore
            }

            // mimic RESUME_OK frame received
            store.releaseFrames(receivedPosition.get());
            disposableReference.set(store.resumeStream().subscribe(consumer));

            // disconnect
            disposableReference.get().dispose();

            while (InMemoryResumableFramesStore.isWorkInProgress(store.state)) {
              // ignore
            }

            // mimic RESUME_OK frame received
            store.releaseFrames(receivedPosition.get());
            disposableReference.set(store.resumeStream().subscribe(consumer));
          },
          () -> {
            frames.onNext(byteBuf1);
            frames.onNextPrioritized(byteBuf11);
            frames.onNext(byteBuf2);
            frames.onNext(byteBuf3);
            frames.onNextPrioritized(byteBuf31);
            frames.onNext(byteBuf4);
            frames.onNext(byteBuf5);
          },
          () -> {
            frames.onNextPrioritized(byteBuf21);
            frames.onNextPrioritized(byteBuf41);
            frames.onNextPrioritized(byteBuf51);
          });

      store.releaseFrames(receivedFrames.stream().mapToInt(ByteBuf::readableBytes).sum());

      assertThat(store.cacheSize).isZero();
      assertThat(store.cachedFrames).isEmpty();

      assertThat(receivedFrames)
          .hasSize(5)
          .containsSequence(byteBuf1, byteBuf2, byteBuf3, byteBuf4, byteBuf5);
      receivedFrames.forEach(ReferenceCounted::release);

      assertThat(byteBuf1.refCnt()).isZero();
      assertThat(byteBuf11.refCnt()).isZero();
      assertThat(byteBuf2.refCnt()).isZero();
      assertThat(byteBuf21.refCnt()).isZero();
      assertThat(byteBuf3.refCnt()).isZero();
      assertThat(byteBuf31.refCnt()).isZero();
      assertThat(byteBuf4.refCnt()).isZero();
      assertThat(byteBuf41.refCnt()).isZero();
      assertThat(byteBuf5.refCnt()).isZero();
      assertThat(byteBuf51.refCnt()).isZero();
    }
  }

  @ParameterizedTest(
      name = "Sending vs Reconnect with incorrect position Race Test. WithLateSubscriber[{0}]")
  @ValueSource(booleans = {true, false})
  void incorrectReleaseFramesWithOnNextRaceTest(boolean withLateSubscriber) {
    Hooks.onErrorDropped(t -> {});
    try {
      for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
        final InMemoryResumableFramesStore store = inMemoryStore(Integer.MAX_VALUE);
        final UnboundedProcessor frames = new UnboundedProcessor();

        store.saveFrames(frames).subscribe();

        final AtomicInteger terminationCnt = new AtomicInteger();
        final Consumer<ByteBuf> consumer = ReferenceCounted::release;
        final Consumer<Throwable> errorConsumer = __ -> terminationCnt.incrementAndGet();
        final AtomicReference<Disposable> disposableReference =
            new AtomicReference<>(
                withLateSubscriber
                    ? null
                    : store.resumeStream().subscribe(consumer, errorConsumer));

        final ByteBuf byteBuf1 = fakeResumableFrame(5);
        final ByteBuf byteBuf11 = fakeConnectionFrame(5);
        final ByteBuf byteBuf2 = fakeResumableFrame(6);
        final ByteBuf byteBuf21 = fakeConnectionFrame(5);
        final ByteBuf byteBuf3 = fakeResumableFrame(7);
        final ByteBuf byteBuf31 = fakeConnectionFrame(5);
        final ByteBuf byteBuf4 = fakeResumableFrame(8);
        final ByteBuf byteBuf41 = fakeConnectionFrame(5);
        final ByteBuf byteBuf5 = fakeResumableFrame(25);
        final ByteBuf byteBuf51 = fakeConnectionFrame(35);

        RaceTestUtils.race(
            () -> {
              if (withLateSubscriber) {
                disposableReference.set(store.resumeStream().subscribe(consumer, errorConsumer));
              }
              // disconnect
              disposableReference.get().dispose();

              // mimic RESUME_OK frame received but with incorrect position
              store.releaseFrames(25);
              disposableReference.set(store.resumeStream().subscribe(consumer, errorConsumer));
            },
            () -> {
              frames.onNext(byteBuf1);
              frames.onNextPrioritized(byteBuf11);
              frames.onNext(byteBuf2);
              frames.onNext(byteBuf3);
              frames.onNextPrioritized(byteBuf31);
              frames.onNext(byteBuf4);
              frames.onNext(byteBuf5);
            },
            () -> {
              frames.onNextPrioritized(byteBuf21);
              frames.onNextPrioritized(byteBuf41);
              frames.onNextPrioritized(byteBuf51);
            });

        assertThat(store.cacheSize).isZero();
        assertThat(store.cachedFrames).isEmpty();
        assertThat(disposableReference.get().isDisposed()).isTrue();
        assertThat(terminationCnt).hasValue(1);

        assertThat(byteBuf1.refCnt()).isZero();
        assertThat(byteBuf11.refCnt()).isZero();
        assertThat(byteBuf2.refCnt()).isZero();
        assertThat(byteBuf21.refCnt()).isZero();
        assertThat(byteBuf3.refCnt()).isZero();
        assertThat(byteBuf31.refCnt()).isZero();
        assertThat(byteBuf4.refCnt()).isZero();
        assertThat(byteBuf41.refCnt()).isZero();
        assertThat(byteBuf5.refCnt()).isZero();
        assertThat(byteBuf51.refCnt()).isZero();
      }
    } finally {
      Hooks.resetOnErrorDropped();
    }
  }

  @ParameterizedTest(
      name =
          "Dispose vs Sending vs Reconnect with incorrect position Race Test. WithLateSubscriber[{0}]")
  @ValueSource(booleans = {true, false})
  void incorrectReleaseFramesWithOnNextWithDisposeRaceTest(boolean withLateSubscriber) {
    Hooks.onErrorDropped(t -> {});
    try {
      for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
        final InMemoryResumableFramesStore store = inMemoryStore(Integer.MAX_VALUE);
        final UnboundedProcessor frames = new UnboundedProcessor();

        store.saveFrames(frames).subscribe();

        final AtomicInteger terminationCnt = new AtomicInteger();
        final Consumer<ByteBuf> consumer = ReferenceCounted::release;
        final Consumer<Throwable> errorConsumer = __ -> terminationCnt.incrementAndGet();
        final AtomicReference<Disposable> disposableReference =
            new AtomicReference<>(
                withLateSubscriber
                    ? null
                    : store.resumeStream().subscribe(consumer, errorConsumer));

        final ByteBuf byteBuf1 = fakeResumableFrame(5);
        final ByteBuf byteBuf11 = fakeConnectionFrame(5);
        final ByteBuf byteBuf2 = fakeResumableFrame(6);
        final ByteBuf byteBuf21 = fakeConnectionFrame(5);
        final ByteBuf byteBuf3 = fakeResumableFrame(7);
        final ByteBuf byteBuf31 = fakeConnectionFrame(5);
        final ByteBuf byteBuf4 = fakeResumableFrame(8);
        final ByteBuf byteBuf41 = fakeConnectionFrame(5);
        final ByteBuf byteBuf5 = fakeResumableFrame(25);
        final ByteBuf byteBuf51 = fakeConnectionFrame(35);

        RaceTestUtils.race(
            () -> {
              if (withLateSubscriber) {
                disposableReference.set(store.resumeStream().subscribe(consumer, errorConsumer));
              }
              // disconnect
              disposableReference.get().dispose();

              // mimic RESUME_OK frame received but with incorrect position
              store.releaseFrames(25);
              disposableReference.set(store.resumeStream().subscribe(consumer, errorConsumer));
            },
            () -> {
              frames.onNext(byteBuf1);
              frames.onNextPrioritized(byteBuf11);
              frames.onNext(byteBuf2);
              frames.onNext(byteBuf3);
              frames.onNextPrioritized(byteBuf31);
              frames.onNext(byteBuf4);
              frames.onNext(byteBuf5);
            },
            () -> {
              frames.onNextPrioritized(byteBuf21);
              frames.onNextPrioritized(byteBuf41);
              frames.onNextPrioritized(byteBuf51);
            },
            store::dispose);

        assertThat(store.cacheSize).isZero();
        assertThat(store.cachedFrames).isEmpty();
        assertThat(disposableReference.get().isDisposed()).isTrue();
        assertThat(terminationCnt).hasValueGreaterThanOrEqualTo(1).hasValueLessThanOrEqualTo(2);

        assertThat(byteBuf1.refCnt()).isZero();
        assertThat(byteBuf11.refCnt()).isZero();
        assertThat(byteBuf2.refCnt()).isZero();
        assertThat(byteBuf21.refCnt()).isZero();
        assertThat(byteBuf3.refCnt()).isZero();
        assertThat(byteBuf31.refCnt()).isZero();
        assertThat(byteBuf4.refCnt()).isZero();
        assertThat(byteBuf41.refCnt()).isZero();
        assertThat(byteBuf5.refCnt()).isZero();
        assertThat(byteBuf51.refCnt()).isZero();
      }
    } finally {
      Hooks.resetOnErrorDropped();
    }
  }

  private int size(ByteBuf... byteBufs) {
    return Arrays.stream(byteBufs).mapToInt(ByteBuf::readableBytes).sum();
  }

  private static InMemoryResumableFramesStore inMemoryStore(int size) {
    return new InMemoryResumableFramesStore("test", Unpooled.EMPTY_BUFFER, size);
  }

  private static ByteBuf fakeResumableFrame(int size) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) 7);
    return Unpooled.wrappedBuffer(bytes);
  }

  private static ByteBuf fakeConnectionFrame(int size) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) 0);
    return Unpooled.wrappedBuffer(bytes);
  }
}
