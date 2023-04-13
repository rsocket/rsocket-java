package io.rsocket.buffer;

import static java.util.concurrent.locks.LockSupport.parkNanos;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ResourceLeakDetector;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Additional Utils which allows to decorate a ByteBufAllocator and track/assertOnLeaks all created
 * ByteBuffs
 */
public class LeaksTrackingByteBufAllocator implements ByteBufAllocator {
  static final Logger LOGGER = LoggerFactory.getLogger(LeaksTrackingByteBufAllocator.class);

  /**
   * Allows to instrument any given the instance of ByteBufAllocator
   *
   * @param allocator
   * @return
   */
  public static LeaksTrackingByteBufAllocator instrument(ByteBufAllocator allocator) {
    return new LeaksTrackingByteBufAllocator(allocator, Duration.ZERO, "");
  }

  /**
   * Allows to instrument any given the instance of ByteBufAllocator
   *
   * @param allocator
   * @return
   */
  public static LeaksTrackingByteBufAllocator instrument(
      ByteBufAllocator allocator, Duration awaitZeroRefCntDuration, String tag) {
    return new LeaksTrackingByteBufAllocator(allocator, awaitZeroRefCntDuration, tag);
  }

  final ConcurrentLinkedQueue<ByteBuf> tracker = new ConcurrentLinkedQueue<>();

  final ByteBufAllocator delegate;

  final Duration awaitZeroRefCntDuration;

  final String tag;

  private LeaksTrackingByteBufAllocator(
      ByteBufAllocator delegate, Duration awaitZeroRefCntDuration, String tag) {
    this.delegate = delegate;
    this.awaitZeroRefCntDuration = awaitZeroRefCntDuration;
    this.tag = tag;
  }

  public LeaksTrackingByteBufAllocator assertHasNoLeaks() {
    try {
      ArrayList<ByteBuf> unreleased = new ArrayList<>();
      for (ByteBuf bb : tracker) {
        if (bb.refCnt() != 0) {
          unreleased.add(bb);
        }
      }

      final Duration awaitZeroRefCntDuration = this.awaitZeroRefCntDuration;
      if (!unreleased.isEmpty() && !awaitZeroRefCntDuration.isZero()) {
        final long startTime = System.currentTimeMillis();
        final long endTimeInMillis = startTime + awaitZeroRefCntDuration.toMillis();
        boolean hasUnreleased;
        while (System.currentTimeMillis() <= endTimeInMillis) {
          hasUnreleased = false;
          for (ByteBuf bb : unreleased) {
            if (bb.refCnt() != 0) {
              hasUnreleased = true;
              break;
            }
          }

          if (!hasUnreleased) {
            return this;
          }

          LOGGER.debug(tag + " await buffers to be released");
          for (int i = 0; i < 100; i++) {
            System.gc();
            parkNanos(1000);
            System.gc();
          }
        }
      }

      Set<ByteBuf> collected = new HashSet<>();
      for (ByteBuf buf : unreleased) {
        if (buf.refCnt() != 0) {
          try {
            collected.add(buf);
          } catch (IllegalReferenceCountException ignored) {
            // fine to ignore if throws because of refCnt
          }
        }
      }

      Assertions.assertThat(
              collected
                  .stream()
                  .filter(bb -> bb.refCnt() != 0)
                  .peek(
                      bb -> {
                        try {
                          LOGGER.debug(tag + " " + resolveTrackingInfo(bb));
                        } catch (Exception e) {
                          e.printStackTrace();
                        }
                      }))
          .describedAs("[" + tag + "] all buffers expected to be released but got ")
          .isEmpty();
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

  static final Class<?> simpleLeakAwareCompositeByteBufClass;
  static final Field leakFieldForComposite;
  static final Class<?> simpleLeakAwareByteBufClass;
  static final Field leakFieldForNormal;
  static final Field allLeaksField;

  static {
    try {
      {
        final Class<?> aClass = Class.forName("io.netty.buffer.SimpleLeakAwareCompositeByteBuf");
        final Field leakField = aClass.getDeclaredField("leak");

        leakField.setAccessible(true);

        simpleLeakAwareCompositeByteBufClass = aClass;
        leakFieldForComposite = leakField;
      }

      {
        final Class<?> aClass = Class.forName("io.netty.buffer.SimpleLeakAwareByteBuf");
        final Field leakField = aClass.getDeclaredField("leak");

        leakField.setAccessible(true);

        simpleLeakAwareByteBufClass = aClass;
        leakFieldForNormal = leakField;
      }

      {
        final Class<?> aClass =
            Class.forName("io.netty.util.ResourceLeakDetector$DefaultResourceLeak");
        final Field field = aClass.getDeclaredField("allLeaks");

        field.setAccessible(true);

        allLeaksField = field;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  static Set<Object> resolveTrackingInfo(ByteBuf byteBuf) throws Exception {
    if (ResourceLeakDetector.getLevel().ordinal()
        >= ResourceLeakDetector.Level.ADVANCED.ordinal()) {
      if (simpleLeakAwareCompositeByteBufClass.isInstance(byteBuf)) {
        return (Set<Object>) allLeaksField.get(leakFieldForComposite.get(byteBuf));
      } else if (simpleLeakAwareByteBufClass.isInstance(byteBuf)) {
        return (Set<Object>) allLeaksField.get(leakFieldForNormal.get(byteBuf));
      }
    }

    return Collections.emptySet();
  }
}
