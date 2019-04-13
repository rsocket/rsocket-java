package io.rsocket.buffer;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import sun.misc.Unsafe;

abstract class BufferUtil {

  private static final Unsafe UNSAFE;

  static {
    Unsafe unsafe;
    try {
      final PrivilegedExceptionAction<Unsafe> action =
          () -> {
            final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);

            return (Unsafe) f.get(null);
          };

      unsafe = AccessController.doPrivileged(action);
    } catch (final Exception ex) {
      throw new RuntimeException(ex);
    }

    UNSAFE = unsafe;
  }

  private static final long BYTE_BUFFER_ADDRESS_FIELD_OFFSET;

  static {
    try {
      BYTE_BUFFER_ADDRESS_FIELD_OFFSET =
          UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
    } catch (final Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Allocate a new direct {@link ByteBuffer} that is aligned on a given alignment boundary.
   *
   * @param capacity required for the buffer.
   * @param alignment boundary at which the buffer should begin.
   * @return a new {@link ByteBuffer} with the required alignment.
   * @throws IllegalArgumentException if the alignment is not a power of 2.
   */
  static ByteBuffer allocateDirectAligned(final int capacity, final int alignment) {
    if (alignment == 0) {
      return ByteBuffer.allocateDirect(capacity);
    }

    if (!isPowerOfTwo(alignment)) {
      throw new IllegalArgumentException("Must be a power of 2: alignment=" + alignment);
    }

    final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity + alignment);

    final long address = UNSAFE.getLong(buffer, BYTE_BUFFER_ADDRESS_FIELD_OFFSET);
    final int remainder = (int) (address & (alignment - 1));
    final int offset = alignment - remainder;

    buffer.limit(capacity + offset);
    buffer.position(offset);

    return buffer.slice();
  }

  private static boolean isPowerOfTwo(final int value) {
    return value > 0 && ((value & (~value + 1)) == value);
  }

  private BufferUtil() {}
}
