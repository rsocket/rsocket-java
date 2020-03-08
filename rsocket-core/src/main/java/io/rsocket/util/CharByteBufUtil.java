package io.rsocket.util;

import static io.netty.util.internal.StringUtil.isSurrogate;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.MathUtil;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.Arrays;

public class CharByteBufUtil {

  private static final byte WRITE_UTF_UNKNOWN = (byte) '?';

  private CharByteBufUtil() {}

  /**
   * Returns the exact bytes length of UTF8 character sequence.
   *
   * <p>This method is producing the exact length according to {@link #writeUtf8(ByteBuf, char[])}.
   */
  public static int utf8Bytes(final char[] seq) {
    return utf8ByteCount(seq, 0, seq.length);
  }

  /**
   * This method is producing the exact length according to {@link #writeUtf8(ByteBuf, char[], int,
   * int)}.
   */
  public static int utf8Bytes(final char[] seq, int start, int end) {
    return utf8ByteCount(checkCharSequenceBounds(seq, start, end), start, end);
  }

  private static int utf8ByteCount(final char[] seq, int start, int end) {
    int i = start;
    // ASCII fast path
    while (i < end && seq[i] < 0x80) {
      ++i;
    }
    // !ASCII is packed in a separate method to let the ASCII case be smaller
    return i < end ? (i - start) + utf8BytesNonAscii(seq, i, end) : i - start;
  }

  private static int utf8BytesNonAscii(final char[] seq, final int start, final int end) {
    int encodedLength = 0;
    for (int i = start; i < end; i++) {
      final char c = seq[i];
      // making it 100% branchless isn't rewarding due to the many bit operations necessary!
      if (c < 0x800) {
        // branchless version of: (c <= 127 ? 0:1) + 1
        encodedLength += ((0x7f - c) >>> 31) + 1;
      } else if (isSurrogate(c)) {
        if (!Character.isHighSurrogate(c)) {
          encodedLength++;
          // WRITE_UTF_UNKNOWN
          continue;
        }
        final char c2;
        try {
          // Surrogate Pair consumes 2 characters. Optimistically try to get the next character to
          // avoid
          // duplicate bounds checking with charAt.
          c2 = seq[++i];
        } catch (IndexOutOfBoundsException ignored) {
          encodedLength++;
          // WRITE_UTF_UNKNOWN
          break;
        }
        if (!Character.isLowSurrogate(c2)) {
          // WRITE_UTF_UNKNOWN + (Character.isHighSurrogate(c2) ? WRITE_UTF_UNKNOWN : c2)
          encodedLength += 2;
          continue;
        }
        // See http://www.unicode.org/versions/Unicode7.0.0/ch03.pdf#G2630.
        encodedLength += 4;
      } else {
        encodedLength += 3;
      }
    }
    return encodedLength;
  }

  private static char[] checkCharSequenceBounds(char[] seq, int start, int end) {
    if (MathUtil.isOutOfBounds(start, end - start, seq.length)) {
      throw new IndexOutOfBoundsException(
          "expected: 0 <= start("
              + start
              + ") <= end ("
              + end
              + ") <= seq.length("
              + seq.length
              + ')');
    }
    return seq;
  }

  /**
   * Encode a {@link char[]} in <a href="http://en.wikipedia.org/wiki/UTF-8">UTF-8</a> and write it
   * into {@link ByteBuf}.
   *
   * <p>This method returns the actual number of bytes written.
   */
  public static int writeUtf8(ByteBuf buf, char[] seq) {
    return writeUtf8(buf, seq, 0, seq.length);
  }

  /**
   * Equivalent to <code>{@link #writeUtf8(ByteBuf, char[])
   * writeUtf8(buf, seq.subSequence(start, end), reserveBytes)}</code> but avoids subsequence object
   * allocation if possible.
   *
   * @return actual number of bytes written
   */
  public static int writeUtf8(ByteBuf buf, char[] seq, int start, int end) {
    return writeUtf8(buf, buf.writerIndex(), checkCharSequenceBounds(seq, start, end), start, end);
  }

  // Fast-Path implementation
  static int writeUtf8(ByteBuf buffer, int writerIndex, char[] seq, int start, int end) {
    int oldWriterIndex = writerIndex;

    // We can use the _set methods as these not need to do any index checks and reference checks.
    // This is possible as we called ensureWritable(...) before.
    for (int i = start; i < end; i++) {
      char c = seq[i];
      if (c < 0x80) {
        buffer.setByte(writerIndex++, (byte) c);
      } else if (c < 0x800) {
        buffer.setByte(writerIndex++, (byte) (0xc0 | (c >> 6)));
        buffer.setByte(writerIndex++, (byte) (0x80 | (c & 0x3f)));
      } else if (isSurrogate(c)) {
        if (!Character.isHighSurrogate(c)) {
          buffer.setByte(writerIndex++, WRITE_UTF_UNKNOWN);
          continue;
        }
        final char c2;
        if (seq.length > ++i) {
          // Surrogate Pair consumes 2 characters. Optimistically try to get the next character to
          // avoid
          // duplicate bounds checking with charAt. If an IndexOutOfBoundsException is thrown we
          // will
          // re-throw a more informative exception describing the problem.
          c2 = seq[i];
        } else {
          buffer.setByte(writerIndex++, WRITE_UTF_UNKNOWN);
          break;
        }
        // Extra method to allow inlining the rest of writeUtf8 which is the most likely code path.
        writerIndex = writeUtf8Surrogate(buffer, writerIndex, c, c2);
      } else {
        buffer.setByte(writerIndex++, (byte) (0xe0 | (c >> 12)));
        buffer.setByte(writerIndex++, (byte) (0x80 | ((c >> 6) & 0x3f)));
        buffer.setByte(writerIndex++, (byte) (0x80 | (c & 0x3f)));
      }
    }
    buffer.writerIndex(writerIndex);
    return writerIndex - oldWriterIndex;
  }

  private static int writeUtf8Surrogate(ByteBuf buffer, int writerIndex, char c, char c2) {
    if (!Character.isLowSurrogate(c2)) {
      buffer.setByte(writerIndex++, WRITE_UTF_UNKNOWN);
      buffer.setByte(writerIndex++, Character.isHighSurrogate(c2) ? WRITE_UTF_UNKNOWN : c2);
      return writerIndex;
    }
    int codePoint = Character.toCodePoint(c, c2);
    // See http://www.unicode.org/versions/Unicode7.0.0/ch03.pdf#G2630.
    buffer.setByte(writerIndex++, (byte) (0xf0 | (codePoint >> 18)));
    buffer.setByte(writerIndex++, (byte) (0x80 | ((codePoint >> 12) & 0x3f)));
    buffer.setByte(writerIndex++, (byte) (0x80 | ((codePoint >> 6) & 0x3f)));
    buffer.setByte(writerIndex++, (byte) (0x80 | (codePoint & 0x3f)));
    return writerIndex;
  }

  public static char[] readUtf8(ByteBuf byteBuf, int length) {
    CharsetDecoder charsetDecoder = CharsetUtil.UTF_8.newDecoder();
    int en = (int) (length * (double) charsetDecoder.maxCharsPerByte());
    char[] ca = new char[en];

    CharBuffer charBuffer = CharBuffer.wrap(ca);
    ByteBuffer byteBuffer =
        byteBuf.nioBufferCount() == 1
            ? byteBuf.internalNioBuffer(byteBuf.readerIndex(), length)
            : byteBuf.nioBuffer(byteBuf.readerIndex(), length);
    byteBuffer.mark();
    try {
      CoderResult cr = charsetDecoder.decode(byteBuffer, charBuffer, true);
      if (!cr.isUnderflow()) cr.throwException();
      cr = charsetDecoder.flush(charBuffer);
      if (!cr.isUnderflow()) cr.throwException();

      byteBuffer.reset();
      byteBuf.skipBytes(length);

      return safeTrim(charBuffer.array(), charBuffer.position());
    } catch (CharacterCodingException x) {
      // Substitution is always enabled,
      // so this shouldn't happen
      throw new IllegalStateException("unable to decode char array from the given buffer", x);
    }
  }

  private static char[] safeTrim(char[] ca, int len) {
    if (len == ca.length) return ca;
    else return Arrays.copyOf(ca, len);
  }
}
