package io.rsocket.transport.shm.io;

import io.rsocket.transport.shm.buffer.ReaderBuffer;
import io.rsocket.transport.shm.buffer.WriterBuffer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.Vector;

public class Socket {

  private static Thread hook;

  private static final Vector<Socket> sockets = new Vector<Socket>();

  private final ReaderBuffer reader;

  private final WriterBuffer writer;

  Socket(ReaderBuffer reader, WriterBuffer writer) {
    this.reader = reader;
    this.writer = writer;
    addShutdownHook(this);
  }

  /**
   * Connects to specified port on local host (loopback interface) and returns a pseudo-socket using
   * shared memory to communicate.
   *
   * @param port
   * @throws IOException
   */
  public Socket(int port) throws IOException {
    java.net.Socket s = new java.net.Socket(InetAddress.getLoopbackAddress(), port);
    // allows to wakeup if server never does the handshake
    s.setSoTimeout(5000);

    DataOutputStream out = new DataOutputStream(s.getOutputStream());
    out.writeInt(Server.HANDSHAKE_PACKET);
    out.flush();

    DataInputStream in = new DataInputStream(s.getInputStream());
    int magic = 0;
    try {
      magic = in.readInt();
    } catch (SocketTimeoutException timeout) {
    }

    if (magic != Server.HANDSHAKE_PACKET) {
      s.close();
      throw new IOException("Server does not support RSocket Shared Memory transport");
    }

    File r = new File(in.readUTF());
    File w = new File(in.readUTF());

    MappedFileSupport jfr = new MappedFileSupport(r);
    MappedFileSupport jfw = new MappedFileSupport(w);

    jfr.deleteFile();
    jfw.deleteFile();

    out.writeInt(0);
    s.close();

    this.reader = jfr.reader();
    this.writer = jfw.writer();

    addShutdownHook(this);
  }

  private static synchronized void addShutdownHook(Socket s) {

    if (hook == null) {
      hook =
          new Thread("shutdown-hook") {
            @Override
            public void run() {
              for (Socket s : sockets) {
                s.close();
              }
            }
          };
      Runtime.getRuntime().addShutdownHook(hook);
    }
    sockets.add(s);
  }

  public ReaderBuffer getReader() {
    return reader;
  }

  public WriterBuffer getWriter() {
    return writer;
  }

  public void close() {
    reader.close();
    writer.close();
  }
}
