package io.rsocket;

import io.rsocket.test.util.LocalDuplexConnection;
import io.rsocket.util.PayloadImpl;
import java.util.ArrayList;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SocketResource {
  private Function<Payload, Mono<Payload>> acceptor;

  private final RSocket requestAcceptor;
  private final RSocketServer srs;

  public final RSocketClient crs;
  public final ArrayList<Throwable> clientErrors = new ArrayList<>();
  public final ArrayList<Throwable> serverErrors = new ArrayList<>();

  public SocketResource() {
    final DirectProcessor<Frame> serverProcessor = DirectProcessor.create();
    final DirectProcessor<Frame> clientProcessor = DirectProcessor.create();

    acceptor = Mono::just;

    requestAcceptor =
        new AbstractRSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            return acceptor.apply(payload);
          }

          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            Flux.from(payloads).map(payload -> impl(payload)).subscribe();

            return Flux.range(1, 10).map(payload -> impl(payload));
          }
        };

    srs =
        new RSocketServer(
            new LocalDuplexConnection("server", clientProcessor, serverProcessor),
            requestAcceptor,
            serverErrors::add);

    crs =
        new RSocketClient(
            new LocalDuplexConnection("client", serverProcessor, clientProcessor),
            clientErrors::add,
            StreamIdSupplier.clientSupplier());
  }

  private <T> PayloadImpl impl(T payload) {
    return new PayloadImpl("server got -> [" + payload.toString() + "]");
  }

  public void acceptor(Function<Payload, Mono<Payload>> acceptor) {
    this.acceptor = acceptor;
  }
}
