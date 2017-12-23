package io.rsocket.transport.netty.server;

import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.http.server.HttpServerRoutes;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;

public class WebsocketRouteTransport implements ServerTransport<Closeable> {
  private HttpServer server;
  private Consumer<? super HttpServerRoutes> routesBuilder;
  private String path;

  public WebsocketRouteTransport(HttpServer server,
                                 Consumer<? super HttpServerRoutes> routesBuilder,
                                 String path) {
    this.server = server;
    this.routesBuilder = routesBuilder;
    this.path = path;
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    return server
        .newRouter(routes -> {
          routesBuilder.accept(routes);
          routes.ws(path, newHandler(acceptor));
        })
        .map(NettyContextCloseable::new);
  }

  public static BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>> newHandler(
      ConnectionAcceptor acceptor) {
    return (in, out) -> {
      WebsocketDuplexConnection connection = new WebsocketDuplexConnection(in, out, in.context());
      acceptor.apply(connection).subscribe();

      return out.neverComplete();
    };
  }
}
