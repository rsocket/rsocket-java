package io.rsocket.tckdrivers.runner;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.uri.UriTransportRegistry;
import java.util.function.Function;

public class TckClient {
  public static RSocket connect(String serverUrl, Function<RSocket, RSocket> acceptor) {
    return RSocketFactory.connect()
        .keepAlive()
        .acceptor(acceptor)
        .transport(UriTransportRegistry.clientForUri(serverUrl))
        .start()
        .block();
  }
}
