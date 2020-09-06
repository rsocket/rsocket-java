package io.rsocket.test;

import static java.util.concurrent.locks.LockSupport.parkNanos;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.assertj.core.api.Assertions;

/**
 * Additional Utils which allows to decorate a ByteBufAllocator and track/assertOnLeaks all created
 * ByteBuffs
 */
class LeaksTrackingByteBufAllocator implements ByteBufAllocator {

  /**
   * Allows to instrument any given the instance of ByteBufAllocator
   *
   * @param allocator
   * @return
   */
  public static LeaksTrackingByteBufAllocator instrument(ByteBufAllocator allocator) {
    return new LeaksTrackingByteBufAllocator(allocator, Duration.ZERO);
  }

  /**
   * Allows to instrument any given the instance of ByteBufAllocator
   *
   * @param allocator
   * @return
   */
  public static LeaksTrackingByteBufAllocator instrument(
      ByteBufAllocator allocator, Duration awaitZeroRefCntDuration) {
    return new LeaksTrackingByteBufAllocator(allocator, awaitZeroRefCntDuration);
  }

  final ConcurrentLinkedQueue<ByteBuf> tracker = new ConcurrentLinkedQueue<>();

  final ByteBufAllocator delegate;

  final Duration awaitZeroRefCntDuration;

  private LeaksTrackingByteBufAllocator(
      ByteBufAllocator delegate, Duration awaitZeroRefCntDuration) {
    this.delegate = delegate;
    this.awaitZeroRefCntDuration = awaitZeroRefCntDuration;
  }

  public LeaksTrackingByteBufAllocator assertHasNoLeaks() {
    try {
      Assertions.assertThat(tracker)
          .allSatisfy(
              buf ->
                  Assertions.assertThat(buf)
                      .matches(
                          bb -> {
                            final Duration awaitZeroRefCntDuration = this.awaitZeroRefCntDuration;
                            if (!awaitZeroRefCntDuration.isZero()) {
                              long end = System.nanoTime() + awaitZeroRefCntDuration.toNanos();
                              while (bb.refCnt() != 0) {
                                if (System.nanoTime() >= end) {
                                  break;
                                }
                                parkNanos(100);
                              }
                            }
                            return bb.refCnt() == 0;
                          },
                          "buffer should be released"));
    } finally {
      tracker.clear();
    }
    return this;
  }

  // Delegating logic with tracking of buffers

  @Override
  public ByteBuf buffer() {
    return track(delegate.buffer());
  }

  @Override
  public ByteBuf buffer(int initialCapacity) {
    return track(delegate.buffer(initialCapacity));
  }

  @Override
  public ByteBuf buffer(int initialCapacity, int maxCapacity) {
    return track(delegate.buffer(initialCapacity, maxCapacity));
  }

  @Override
  public ByteBuf ioBuffer() {
    return track(delegate.ioBuffer());
  }

  @Override
  public ByteBuf ioBuffer(int initialCapacity) {
    return track(delegate.ioBuffer(initialCapacity));
  }

  @Override
  public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
    return track(delegate.ioBuffer(initialCapacity, maxCapacity));
  }

  @Override
  public ByteBuf heapBuffer() {
    return track(delegate.heapBuffer());
  }

  @Override
  public ByteBuf heapBuffer(int initialCapacity) {
    return track(delegate.heapBuffer(initialCapacity));
  }

  @Override
  public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
    return track(delegate.heapBuffer(initialCapacity, maxCapacity));
  }

  @Override
  public ByteBuf directBuffer() {
    return track(delegate.directBuffer());
  }

  @Override
  public ByteBuf directBuffer(int initialCapacity) {
    return track(delegate.directBuffer(initialCapacity));
  }

  @Override
  public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
    return track(delegate.directBuffer(initialCapacity, maxCapacity));
  }

  @Override
  public CompositeByteBuf compositeBuffer() {
    return track(delegate.compositeBuffer());
  }

  @Override
  public CompositeByteBuf compositeBuffer(int maxNumComponents) {
    return track(delegate.compositeBuffer(maxNumComponents));
  }

  @Override
  public CompositeByteBuf compositeHeapBuffer() {
    return track(delegate.compositeHeapBuffer());
  }

  @Override
  public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
    return track(delegate.compositeHeapBuffer(maxNumComponents));
  }

  @Override
  public CompositeByteBuf compositeDirectBuffer() {
    return track(delegate.compositeDirectBuffer());
  }

  @Override
  public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
    return track(delegate.compositeDirectBuffer(maxNumComponents));
  }

  @Override
  public boolean isDirectBufferPooled() {
    return delegate.isDirectBufferPooled();
  }

  @Override
  public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
    return delegate.calculateNewCapacity(minNewCapacity, maxCapacity);
  }

  <T extends ByteBuf> T track(T buffer) {
    tracker.offer(buffer);

    return buffer;
  }
}
