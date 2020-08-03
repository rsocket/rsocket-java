package io.rsocket.transport.shm.buffer;

import static io.rsocket.transport.shm.buffer.Const.LEN_PACKET_INFO;
import static io.rsocket.transport.shm.buffer.Const.PACKET_INFO;
import static io.rsocket.transport.shm.buffer.Const.RSEQ;
import static io.rsocket.transport.shm.buffer.Const.WSEQ;

import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import java.nio.ByteBuffer;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

public class ReaderBuffer extends UnsafeBuffer implements Closeable {

  private int rseq;

  private int wseq;

  private final WrappedDirectBufferByteBuf wrappedDirectBufferByteBuf;

  private boolean closed;

  /** Number of data bytes that can be stored in buffer. Must be a power of 2. */
  protected final int capacity;

  /** Equals (capacity - 1). Allows to perform quick modulo with binary AND. */
  protected final int dataMask;

  /** Maximum number of packets that can be written but not read. */
  protected final int npackets;

  /** Equals (npackets - 1). Allows to perform quick modulo with binary AND. */
  protected final int packetMask;

  protected final int dataOffset;

  public ReaderBuffer(ByteBuffer buf, int npackets) {
    super(buf);

    this.npackets = npackets;
    this.packetMask = npackets - 1;
    this.capacity = buf.capacity() - (PACKET_INFO + npackets * LEN_PACKET_INFO);
    if (Integer.bitCount(capacity) != 1)
      throw new IllegalArgumentException("Buffer capacity for data must be a power of 2");
    this.dataMask = capacity - 1;
    this.dataOffset = PACKET_INFO + npackets * LEN_PACKET_INFO;
    this.wrappedDirectBufferByteBuf = new WrappedDirectBufferByteBuf(capacity);
  }

  @Override
  public void close() {
    if (!closed) {
      this.closed = true;
      putInt(RSEQ, -1);
      BufferUtil.free(this);
    }
  }

  public boolean isClosed() {
    return closed;
  }

  public ByteBuf read() {
    if (wseq <= rseq) {
      readWseq();

      if (wseq <= rseq) {
        if (wseq < 0) throw new ClosedException("Socket closed");
        return null;
      }
    }

    final int pktInfo = PACKET_INFO + (rseq & packetMask) * LEN_PACKET_INFO;
    final int packetSize = getInt(pktInfo + 4);
    final int packetPos = dataOffset + (getInt(pktInfo) & dataMask);

    wrappedDirectBufferByteBuf.wrap(this, packetPos, packetPos + packetSize);
    return wrappedDirectBufferByteBuf;
  }

  public void advance(int packetsCount) {
    rseq += packetsCount;
    putInt(RSEQ, rseq);
  }

  private void readWseq() {
    wseq = getInt(WSEQ);
  }

  public int available() {
    int wseq = getInt(WSEQ);
    if (wseq <= rseq) return 0;

    int windex = (wseq - 1) & packetMask; // last packet written
    int rindex = rseq & packetMask; // first packet written

    int start = getInt(PACKET_INFO + rindex * LEN_PACKET_INFO);
    int end =
        getInt(PACKET_INFO + windex * LEN_PACKET_INFO)
            + getInt(PACKET_INFO + windex * LEN_PACKET_INFO + 4);

    if (start <= end) return end - start;
    else return capacity - (start - end);
  }
}
