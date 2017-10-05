package io.rsocket.integration;

import io.rsocket.*;
import io.rsocket.exceptions.ApplicationException;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TestingStreaming {
  Supplier<ServerTransport<? extends Closeable>> serverSupplier =
      () -> LocalServerTransport.create("test");

  Supplier<ClientTransport> clientSupplier = () -> LocalClientTransport.create("test");

  @Test(expected = ApplicationException.class)
  public void testRangeButThrowException() throws Exception {
    Closeable server = null;
    try {
      server =
          RSocketFactory.receive()
              .errorConsumer(t -> t.printStackTrace())
              .acceptor(
                  (connectionSetupPayload, rSocket) -> {
                    AbstractRSocket abstractRSocket =
                        new AbstractRSocket() {
                          @Override
                          public double availability() {
                            return 1.0;
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            return Flux.range(1, 1000)
                                .doOnNext(
                                    i -> {
                                      if (i > 3) {
                                        throw new RuntimeException("BOOM!");
                                      }
                                    })
                                .map(l -> new PayloadImpl("l -> " + l))
                                .cast(Payload.class);
                          }
                        };

                    return Mono.just(abstractRSocket);
                  })
              .transport(serverSupplier.get())
              .start()
              .block();

      Flux.range(1, 6).flatMap(i -> consumer("connection number -> " + i)).blockLast();
      System.out.println("here");

    } finally {
      server.close().block();
    }
  }

  @Test
  public void testRangeOfConsumers() throws Exception {
    Closeable server = null;
    try {
      server =
          RSocketFactory.receive()
              .errorConsumer(t -> t.printStackTrace())
              .acceptor(
                  (connectionSetupPayload, rSocket) -> {
                    AbstractRSocket abstractRSocket =
                        new AbstractRSocket() {
                          @Override
                          public double availability() {
                            return 1.0;
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            return Flux.range(1, 1000)
                                .map(l -> new PayloadImpl("l -> " + l))
                                .cast(Payload.class);
                          }
                        };

                    return Mono.just(abstractRSocket);
                  })
              .transport(serverSupplier.get())
              .start()
              .block();

      Flux.range(1, 6).flatMap(i -> consumer("connection number -> " + i)).blockLast();
      System.out.println("here");

    } finally {
      server.close().block();
    }
  }

  public Flux<Payload> consumer(String s) {
    Mono<RSocket> test =
        RSocketFactory.connect()
            .errorConsumer(t -> t.printStackTrace())
            .transport(clientSupplier)
            .start();

    Flux<Payload> payloadFlux =
        test.flatMapMany(
            rSocket -> {
              AtomicInteger count = new AtomicInteger();
              return Flux.range(1, 100)
                  .flatMap(
                      i -> {
                        return rSocket.requestStream(new PayloadImpl("i -> " + i)).take(100);
                      },
                      1);
            });

    return payloadFlux;
  }

  @Test
  public void testSingleConsumer() throws Exception {
    Closeable server = null;

    try {
      server =
          RSocketFactory.receive()
              .acceptor(
                  (connectionSetupPayload, rSocket) -> {
                    AbstractRSocket abstractRSocket =
                        new AbstractRSocket() {
                          @Override
                          public double availability() {
                            return 1.0;
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            return Flux.range(1, 10_000)
                                .map(l -> new PayloadImpl("l -> " + l))
                                .cast(Payload.class);
                          }
                        };

                    return Mono.just(abstractRSocket);
                  })
              .transport(serverSupplier.get())
              .start()
              .block();

      consumer("1").blockLast();

    } finally {
      server.close().block();
    }
  }

  @Test
  public void testFluxOnly() {
    Flux<Long> longFlux = Flux.interval(Duration.ofMillis(1)).onBackpressureDrop();

    Flux.range(1, 60).flatMap(i -> longFlux.take(1000)).blockLast();
  }
}
