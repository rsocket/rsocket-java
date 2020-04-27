/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.examples.transport.tcp.resume;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class ResumeFileTransfer {
  /*amount of file chunks requested by subscriber: n, refilled on n/2 of received items*/
  private static final int PREFETCH_WINDOW_SIZE = 4;

  public static void main(String[] args) {
    RequestCodec requestCodec = new RequestCodec();
    Resume resume =
        new Resume()
            .sessionDuration(Duration.ofMinutes(5))
            .retry(
                Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1))
                    .doBeforeRetry(
                        retrySignal ->
                            System.out.println("Disconnected. Trying to resume connection...")));

    CloseableChannel server =
        RSocketServer.create((setup, rSocket) -> Mono.just(new FileServer(requestCodec)))
            .resume(resume)
            .bind(TcpServerTransport.create("localhost", 8000))
            .block();

    RSocket client =
        RSocketConnector.create()
            .resume(resume)
            .connect(TcpClientTransport.create("localhost", 8001))
            .block();

    client
        .requestStream(requestCodec.encode(new Request(16, "lorem.txt")))
        .doFinally(s -> server.dispose())
        .subscribe(Files.fileSink("rsocket-examples/out/lorem_output.txt", PREFETCH_WINDOW_SIZE));

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
