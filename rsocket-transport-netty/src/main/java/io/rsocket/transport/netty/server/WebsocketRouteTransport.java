package io.rsocket.transport.netty.server;

import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import io.rsocket.util.CloseableAdapter;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServerRoutes;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

public class WebsocketRouteTransport implements ServerTransport<Closeable> {
  private HttpServerRoutes routes;
  private String path;

  public WebsocketRouteTransport(HttpServerRoutes routes, String path) {
    this.routes = routes;
    this.path = path;
  }

  @Override public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    return Mono.defer(() -> {
      routes.ws(path, newHandler(acceptor));

      return Mono.just(new CloseableAdapter(() -> {
        // TODO close route somehow
      }));
    });
  }

  public static BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> newHandler(
      ConnectionAcceptor acceptor) {
    return (in, out) -> {
      WebsocketDuplexConnection connection =
          new WebsocketDuplexConnection(in, out, in.context());
      acceptor.apply(connection).subscribe();

      return out.neverComplete();
    };
  }
}
