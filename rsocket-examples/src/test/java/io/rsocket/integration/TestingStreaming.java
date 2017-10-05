package io.rsocket.integration;

import io.rsocket.*;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TestingStreaming {
  Supplier<ServerTransport<? extends Closeable>> serverSupplier =
      () -> {
        //return LocalServerTransport.create("test");
        return TcpServerTransport.create(8001);
      };

  Supplier<ClientTransport> clientSupplier =
      () -> {
        //return LocalClientTransport.create("test");
        return TcpClientTransport.create(8001);
      };

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
                                //                              .doOnRequest(l ->
                                // System.out.println("REQUESTED " + l))
                                .map(l -> new PayloadImpl("l -> " + l))
                                .cast(Payload.class);
                            //                              .doOnCancel(
                            //                                  () ->
                            //                                      System.out.println(
                            //                                          "CANCELED "));
                          }
                        };

                    return Mono.just(abstractRSocket);
                  })
              .transport(serverSupplier.get())
              .start()
              .block();

      Flux.range(1, 6)
          // .log()
          .flatMap(i -> consumer("connection number -> " + i))
          .blockLast();
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
                  // .log()
                  // .doOnNext(i -> System.out.println("emitting -> " + i))
                  .flatMap(
                      i -> {
                        //  System.out.println(s + " -> starting number -> " + i);
                        return rSocket
                            .requestStream(new PayloadImpl("i -> " + i))
                            // .log()

                            //                            .doOnRequest(l -> System.out.println(i + " - client---------------------- -> " + l))
                            //                            .doOnCancel(
                            //                                () -> {
                            //                                  System.out.println(
                            //                                      "cancel ????D?FSD?FS?D?FSD?FDS?DFS?DFS?SDF??SDF?SD");
                            //                                })
                            .take(100)
                            //                            .doOnRequest(l -> System.out.println("requested on client -> " + l))
                            //                            .doOnError(Throwable::printStackTrace)
                            .doOnNext(
                                p -> {
                                  String d = p.getDataUtf8();
                                  // System.out.println(s + " -> " + i + " - [" + d + "]");
                                })
                            .doFinally(
                                signalType -> {
                                  //  System.out.println(s + " -> done with -> " + i);
                                });
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
                                //                                .doOnRequest(
                                //                                    l -> System.out.println("server got requested -> " + l))
                                //.doOnCancel(() -> System.out.println("cancel"))
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
