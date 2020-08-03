package io.rsocket.transport.shm.io;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

public class Server implements Closeable {

  private static final int DEFAULT_MAX_PACKETS =
      Integer.getInteger("rsocket.shm.maxPendingPackets", 1024);

  private static final int DEFAULT_CAPACITY =
      Integer.getInteger("rsocket.shm.capacity", 4 * 1024 * 1024);

  static final int HANDSHAKE_PACKET = 0x3C04E7;

  private final java.net.ServerSocket srv;

  private final int maxPackets;

  private final int capacity;

  private boolean closed;

  /**
   * Creates a new Shared Memory RSocket server with default settings.
   *
   * @param port the TCP port on which RSocket Shared Memory Server will listen for connections
   * @throws IOException
   */
  public Server(int port) throws IOException {
    this(port, DEFAULT_MAX_PACKETS, DEFAULT_CAPACITY);
  }

  public Server(int port, int maxPackets, int capacity) throws IOException {
    if (Integer.bitCount(maxPackets) != 1)
      throw new IllegalArgumentException("Max packets must be a power of 2");

    if (Integer.bitCount(capacity) != 1)
      throw new IllegalArgumentException("Capacity must be a power of 2");

    this.maxPackets = maxPackets;
    this.capacity = capacity;
    srv = new java.net.ServerSocket();

    srv.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
  }

  @Override
  public void close() throws IOException {
    closed = true;
    srv.close();
  }

  public Socket accept() throws IOException {
    while (true) {

      if (closed) throw new IllegalStateException("Closed");

      java.net.Socket s = srv.accept();
      // allows to wakeup if client never does the handshake
      s.setSoTimeout(1000);

      DataInputStream in = new DataInputStream(s.getInputStream());
      DataOutputStream out = new DataOutputStream(s.getOutputStream());
      out.writeInt(HANDSHAKE_PACKET);
      out.flush();

      int magic = 0;
      try {
        magic = in.readInt();
      } catch (SocketTimeoutException timeout) {
      }

      if (magic != HANDSHAKE_PACKET) {
        s.close();
        continue;
      }

      // TODO: write parameters in file header (+ misc meta data)
      MappedFileSupport fw = new MappedFileSupport(maxPackets, capacity);
      MappedFileSupport fr = new MappedFileSupport(maxPackets, capacity);

      out.writeUTF(fw.getPath());
      out.writeUTF(fr.getPath());
      out.flush();

      in.readInt();
      fw.deleteFile();
      fr.deleteFile();
      s.close();

      return new Socket(fr.reader(), fw.writer());
    }
  }
}
