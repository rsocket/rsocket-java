package io.rsocket.examples.transport.tcp.resume;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.resume.ClientResume;
import io.rsocket.resume.PeriodicResumeStrategy;
import io.rsocket.resume.ResumeStrategy;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ResumeFileTransfer {

  public static void main(String[] args) {
    RequestCodec requestCodec = new RequestCodec();

    CloseableChannel server =
        RSocketFactory.receive()
            .resume()
            .resumeSessionDuration(Duration.ofMinutes(5))
            .acceptor((setup, rSocket) -> Mono.just(new FileServer(requestCodec)))
            .transport(TcpServerTransport.create("localhost", 8000))
            .start()
            .block();

    RSocket client =
        RSocketFactory.connect()
            .resume()
            .resumeStrategy(
                () -> new VerboseResumeStrategy(new PeriodicResumeStrategy(Duration.ofSeconds(1))))
            .resumeSessionDuration(Duration.ofMinutes(5))
            .transport(TcpClientTransport.create("localhost", 8001))
            .start()
            .block();

    client
        .requestStream(requestCodec.encode(new Request(16, "lorem.txt")))
        .doFinally(s -> server.dispose())
        .subscribe(Files.fileSink("rsocket-examples/out/lorem_output.txt", 256));

    server.onClose().block();
  }

  private static class FileServer extends AbstractRSocket {
    private final RequestCodec requestCodec;

    public FileServer(RequestCodec requestCodec) {
      this.requestCodec = requestCodec;
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      Request request = requestCodec.decode(payload);
      payload.release();
      String fileName = request.getFileName();
      int chunkSize = request.getChunkSize();

      Flux<Long> ticks = Flux.interval(Duration.ofMillis(500)).onBackpressureDrop();

      return Files.fileSource(fileName, chunkSize)
          .map(DefaultPayload::create)
          .zipWith(ticks, (p, tick) -> p);
    }
  }

  private static class VerboseResumeStrategy implements ResumeStrategy {
    private final ResumeStrategy resumeStrategy;

    public VerboseResumeStrategy(ResumeStrategy resumeStrategy) {
      this.resumeStrategy = resumeStrategy;
    }

    @Override
    public Publisher<?> apply(ClientResume clientResume, Throwable throwable) {
      return Flux.from(resumeStrategy.apply(clientResume, throwable))
          .doOnNext(v -> System.out.println("Disconnected. Trying to resume connection..."));
    }
  }

  private static class RequestCodec {

    public Payload encode(Request request) {
      String encoded = request.getChunkSize() + ":" + request.getFileName();
      return DefaultPayload.create(encoded);
    }

    public Request decode(Payload payload) {
      String encoded = payload.getDataUtf8();
      String[] chunkSizeAndFileName = encoded.split(":");
      int chunkSize = Integer.parseInt(chunkSizeAndFileName[0]);
      String fileName = chunkSizeAndFileName[1];
      return new Request(chunkSize, fileName);
    }
  }

  private static class Request {
    private final int chunkSize;
    private final String fileName;

    public Request(int chunkSize, String fileName) {
      this.chunkSize = chunkSize;
      this.fileName = fileName;
    }

    public int getChunkSize() {
      return chunkSize;
    }

    public String getFileName() {
      return fileName;
    }
  }
}
