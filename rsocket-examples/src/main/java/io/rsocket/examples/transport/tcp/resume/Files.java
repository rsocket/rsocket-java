package io.rsocket.examples.transport.tcp.resume;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import java.io.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

class Files {

  public static Flux<ByteBuf> fileSource(String fileName, int chunkSizeBytes) {
    return Flux.generate(
        () -> new FileState(fileName, chunkSizeBytes), FileState::consumeNext, FileState::dispose);
  }

  public static Subscriber<Payload> fileSink(String fileName, int windowSize) {
    return new Subscriber<Payload>() {
      Subscription s;
      int requests = windowSize;
      OutputStream outputStream;
      int receivedBytes;
      int receivedCount;

      @Override
      public void onSubscribe(Subscription s) {
        this.s = s;
        this.s.request(requests);
      }

      @Override
      public void onNext(Payload payload) {
        ByteBuf data = payload.data();
        receivedBytes += data.readableBytes();
        receivedCount += 1;
        System.out.println(
            "Received file chunk: " + receivedCount + ". Total size: " + receivedBytes);
        if (outputStream == null) {
          outputStream = open(fileName);
        }
        write(outputStream, data);
        payload.release();

        requests--;
        if (requests == windowSize / 2) {
          requests += windowSize;
          s.request(windowSize);
        }
      }

      private void write(OutputStream outputStream, ByteBuf byteBuf) {
        try {
          byteBuf.readBytes(outputStream, byteBuf.readableBytes());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void onError(Throwable t) {
        close(outputStream);
      }

      @Override
      public void onComplete() {
        close(outputStream);
      }

      private OutputStream open(String filename) {
        try {
          /*do not buffer for demo purposes*/
          return new FileOutputStream(filename);
        } catch (FileNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      private void close(OutputStream stream) {
        if (stream != null) {
          try {
            stream.close();
          } catch (IOException e) {
          }
        }
      }
    };
  }

  private static class FileState {
    private final String fileName;
    private final int chunkSizeBytes;
    private BufferedInputStream inputStream;
    private byte[] chunkBytes;

    public FileState(String fileName, int chunkSizeBytes) {
      this.fileName = fileName;
      this.chunkSizeBytes = chunkSizeBytes;
    }

    public FileState consumeNext(SynchronousSink<ByteBuf> sink) {
      if (inputStream == null) {
        InputStream in = getClass().getClassLoader().getResourceAsStream(fileName);
        if (in == null) {
          sink.error(new FileNotFoundException(fileName));
          return this;
        }
        this.inputStream = new BufferedInputStream(in);
        this.chunkBytes = new byte[chunkSizeBytes];
      }
      try {
        int consumedBytes = inputStream.read(chunkBytes);
        if (consumedBytes == -1) {
          sink.complete();
        } else {
          sink.next(Unpooled.copiedBuffer(chunkBytes, 0, consumedBytes));
        }
      } catch (IOException e) {
        sink.error(e);
      }
      return this;
    }

    public void dispose() {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
        }
      }
    }
  }
}
