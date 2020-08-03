package io.rsocket.transport.shm.buffer;

import static io.rsocket.transport.shm.buffer.Const.LEN_PACKET_INFO;
import static io.rsocket.transport.shm.buffer.Const.PACKET_INFO;
import static io.rsocket.transport.shm.buffer.Const.RSEQ;
import static io.rsocket.transport.shm.buffer.Const.WSEQ;
import static io.rsocket.transport.shm.buffer.Const._CACHELINE;

import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import java.nio.ByteBuffer;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

public class WriterBuffer extends UnsafeBuffer implements Closeable {

  /** Number of data bytes that can be stored in buffer. Must be a power of 2. */
  protected final int capacity;

  /** Equals (capacity - 1). Allows to perform quick modulo with binary AND. */
  protected final int dataMask;

  /** Maximum number of packets that can be written but not read. */
  protected final int npackets;

  /** Equals (npackets - 1). Allows to perform quick modulo with binary AND. */
  protected final int packetMask;

  protected final int dataOffset;

  /** The sequence number of the next packet to write. */
  private int wseq;

  /** Pending packet's absolute start and end positions. */
  private int pstart, pend;

  /**
   * Equivalent to (pend > pstart) but looks like it is faster to store this information in a
   * dedicated boolean.
   */
  private boolean dirty;

  /**
   * Optional packet alignment. By default we align on cache lines to avoid having several packets
   * in the same cache line, which would cause false sharing (reader and writer threads would access
   * the same line concurrently).
   */
  private int align = _CACHELINE;

  /** Used to modulo on align size. */
  private int alignMask = _CACHELINE - 1;

  private boolean closed;

  public WriterBuffer(ByteBuffer buf, int npackets) {
    super(buf);

    this.npackets = npackets;
    this.packetMask = npackets - 1;
    this.capacity = buf.capacity() - (PACKET_INFO + npackets * LEN_PACKET_INFO);
    if (Integer.bitCount(capacity) != 1)
      throw new IllegalArgumentException("Buffer capacity for data must be a power of 2");
    this.dataMask = capacity - 1;
    this.dataOffset = PACKET_INFO + npackets * LEN_PACKET_INFO;
  }

  /**
   * Sets packet alignment.
   *
   * @param align must be either 0 or a power of 2
   */
  public void setAlign(int align) {
    if (align < 0 || align > 0 && Integer.bitCount(align) != 1)
      throw new IllegalArgumentException("Invalid alignment: " + align);
    this.align = align;
    this.alignMask = align == 0 ? 0 : align - 1;
  }

  public int claim(int size) {
    final int rseq = rseq();
    final int wseq = this.wseq;

    if (rseq == wseq) {
      // reset position in buffer when reader is up to date
      if (rseq > 0 && !dirty) {
        this.pstart = this.pend = 0;
      }
    } else if (rseq < 0) close();

    // cannot write if all packets are written and the reader didn't read them
    else if (wseq - rseq >= npackets) {
      return -1;
    }

    if (isClosed()) throw new ClosedException("Closed");

    int av = getAvailableSpace(rseq);
    if (av < size) {
      return -1;
    }

    int pkt = PACKET_INFO + (wseq & packetMask) * LEN_PACKET_INFO;

    int mod = pend & alignMask;
    this.pend += align - mod;
    this.pstart = pend;

    putInt(pkt, pend);
    return 1;
  }

  /**
   * EXPERIMENTAL. Sends the packet that was previously returned by {@link #claim(int)}.
   *
   * @param packet
   */
  public void write(ByteBuf byteBuf) {
    int size = byteBuf.readableBytes();
    int pkt = PACKET_INFO + (wseq & packetMask) * LEN_PACKET_INFO;
    if (byteBuf.nioBufferCount() == 1) {
      ByteBuffer internalNioBuffer = byteBuf.internalNioBuffer(byteBuf.readerIndex(), size);
      this.putBytes(dataOffset + pstart, internalNioBuffer, size);
    } else {
      final ByteBuffer[] buffers = byteBuf.nioBuffers();
      int index = dataOffset + pstart;
      for (ByteBuffer buffer : buffers) {
        final int remaining = buffer.remaining();
        putBytes(index, buffer, remaining);
        index += remaining;
      }
    }
    putInt(pkt + 4, size);
    pend += size;
    pstart = pend;
    putInt(WSEQ, ++wseq);
  }

  public void commit() {
    // FIXME: provide logic for sending packets in batch
  }

  /**
   * Returns the absolute position of the last read byte.
   *
   * @param rseq reader sequence number.
   */
  private int head(int rseq) {
    // if all packets are read, the position has been or will be reset
    if (wseq == rseq) return 0;

    final int pkt = PACKET_INFO + ((wseq - 1) & packetMask) * LEN_PACKET_INFO;

    return getInt(pkt) + getInt(pkt + 4);
  }

  /**
   * Returns the absolute position of specified packet.
   *
   * @param seq a packet number
   */
  private int start(int seq) {
    return getInt(PACKET_INFO + (seq & packetMask) * LEN_PACKET_INFO);
  }

  /**
   * Returns how many bytes can be written in one single chunk at current position. We can be
   * limited either by the bounds of the ByteBuffer or by how many bytes must still be read.
   *
   * <p>Result is (X - head) where X is the smallest of:
   *
   * <ul>
   *   <li>head + capacity - (head - head % capacity)
   *   <li>tail + capacity
   * </ul>
   *
   * <p>This method works only if pend/pstart are reset to 0 when the reader has read everything
   * (otherwise 0 can be returned instead of capacity).
   *
   * @param rseq sequence number of reader
   * @param head position of last written byte
   */
  private int getAvailableSpace(int rseq) {
    return Math.min(start(rseq), pend - (pend & dataMask)) + capacity - pend;

    // NB: this is the contracted form of:
    // -----------------------------------
    // int lim1 = pend + capacity - (pend & dataMask);
    // int lim2 = start(rseq) + capacity;
    // return Math.min(lim1, lim2) - pend;
  }

  /** Returns the reader sequence number. */
  private final int rseq() {
    return getInt(RSEQ);
  }

  /** Returns how many bytes can be written (mainly for test purposes). */
  public int available() {
    // cannot write if all packets are written and the reader didn't read any
    int rseq = rseq();
    if (wseq - rseq >= npackets) return 0;
    return getAvailableSpace(rseq);
  }

  @Override
  public void close() {
    if (!closed) {
      this.closed = true;
      putInt(WSEQ, -1);
      BufferUtil.free(this);
    }
  }

  public boolean isClosed() {
    return closed;
  }

  /** For testing purposes. */
  public int getSeqNum() {
    return wseq;
  }

  /** For testing purposes. */
  public int getPosition() {
    return head(rseq());
  }

  /** For testing purposes. */
  public String debug() {
    return String.format(
        "wseq=%d rseq=%d pstart=%d plen=%d tail=%d dirty=%b capacity=%d",
        wseq, rseq(), pstart, pend - pstart, head(rseq()), pend > pstart, capacity);
  }
}
