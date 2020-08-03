package io.rsocket.transport.shm.io;

import io.rsocket.transport.shm.buffer.Const;
import io.rsocket.transport.shm.buffer.ReaderBuffer;
import io.rsocket.transport.shm.buffer.WriterBuffer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.UUID;

public class MappedFileSupport implements Const {

  static final String FILE_PREFIX = "rsocket-shm-";

  private final MappedByteBuffer mappedByteBuffer;

  private final RandomAccessFile io;

  private final ReaderBuffer reader;

  private final WriterBuffer writer;

  private final File file;

  /** Creates a new exchange file and associated reader and writer. */
  public MappedFileSupport(int maxPackets, int capacity) throws IOException {
    this(createTempFile(), true, maxPackets, capacity);
  }

  /** Opens and wrap specified exchange file with a reader and writer. */
  public MappedFileSupport(File file) throws IOException {
    this(file, false, -1, -1);
  }

  private MappedFileSupport(File file, boolean create, int maxPackets, int capacity)
      throws IOException {
    if (!create && !file.exists()) throw new FileNotFoundException("File does not exist");

    this.file = file;
    this.io = new RandomAccessFile(file, "rw");

    if (create) {
      int size = capacity + PACKET_INFO + maxPackets * LEN_PACKET_INFO;
      io.setLength(0);

      // append data instead of just setting the size: in case we are using
      // a real filesystem, this could avoid getting a fragmented file
      // (or not)
      for (int i = 0; i < size; i += 1024) {
        io.write(new byte[1024]);
      }
      io.setLength(size);
    }

    FileChannel channel = io.getChannel();
    MappedByteBuffer mappedBuffer =
        this.mappedByteBuffer = channel.map(MapMode.READ_WRITE, 0, io.length());
    mappedBuffer.order(ByteOrder.BIG_ENDIAN);
    mappedBuffer.load();
    channel.close();

    if (create) {
      mappedBuffer.putInt(META_MAX_PACKETS, maxPackets);
      mappedBuffer.putInt(META_CAPACITY, capacity);
      mappedBuffer.force();
    } else {
      maxPackets = mappedBuffer.getInt(META_MAX_PACKETS);
    }

    reader = new ReaderBuffer(mappedBuffer, maxPackets);
    writer = new WriterBuffer(mappedBuffer, maxPackets);

    file.deleteOnExit();
  }

  public ReaderBuffer reader() {
    return reader;
  }

  public WriterBuffer writer() {
    return writer;
  }

  public String getPath() {
    return file.getAbsolutePath();
  }

  /**
   * Deletes the file to make it harder to sniff stream. Can be called (at least under linux) after
   * both endpoints have opened the file.
   */
  public void deleteFile() {
    file.delete();
  }

  private static File createTempFile() throws IOException {
    try {
      // under linux, try to use tmpfs
      File dir = new File("/dev/shm");
      if (dir.exists()) {
        File file = new File(dir, FILE_PREFIX + UUID.randomUUID());
        new FileOutputStream(file).close();
        return file;
      }
    } catch (Exception ignored) {
    }
    return File.createTempFile(FILE_PREFIX, UUID.randomUUID().toString());
  }

  public MappedByteBuffer getBuffer() {
    return mappedByteBuffer;
  }
}
