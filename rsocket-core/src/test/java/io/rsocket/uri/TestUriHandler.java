package io.rsocket.uri;

import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.transport.ClientTransport;
import java.net.URI;
import java.util.Optional;
import reactor.core.publisher.Mono;

public class TestUriHandler implements UriHandler {
  @Override
  public Optional<ClientTransport> buildClient(URI uri) {
    if (uri.getScheme().equals("test")) {
      return Optional.of(() -> Mono.just(new TestDuplexConnection()));
    }
    return UriHandler.super.buildClient(uri);
  }
}
