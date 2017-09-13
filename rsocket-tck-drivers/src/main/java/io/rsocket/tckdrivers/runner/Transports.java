package io.rsocket.tckdrivers.runner;

import io.rsocket.Closeable;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import java.util.concurrent.atomic.AtomicInteger;

public class Transports {
  private static AtomicInteger localCounter = new AtomicInteger();

  public static String actualLocalUrl(String transport, String uri, Closeable closeable) {
    if (transport.equals("tcp")) {
      // TODO get external IP?
      uri = "tcp://localhost:" + nettyPort(closeable);
    } else if (transport.equals("ws")) {
      // TODO get external IP?
      uri = "ws://localhost:" + nettyPort(closeable);
    }
    return uri;
  }

  public static int nettyPort(Closeable closeable) {
    return ((NettyContextCloseable) closeable).address().getPort();
  }

  public static String urlForTransport(String transport) {
    switch (transport) {
      case "local":
        return "local:tck" + localCounter.incrementAndGet();
      case "tcp":
        // TODO get external IP?
        return "tcp://localhost:0";
      case "ws":
        // TODO get external IP?
        return "ws://localhost:0";
      default:
        throw new UnsupportedOperationException("unknown transport '" + transport + "'");
    }
  }
}
