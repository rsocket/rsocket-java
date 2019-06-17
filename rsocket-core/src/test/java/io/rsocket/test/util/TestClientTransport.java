package io.rsocket.test.util;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import reactor.core.publisher.Mono;

public class TestClientTransport implements ClientTransport {

  private final TestDuplexConnection testDuplexConnection = new TestDuplexConnection();

  @Override
  public Mono<DuplexConnection> connect(int mtu) {
    return Mono.just(testDuplexConnection);
  }

  public TestDuplexConnection testConnection() {
    return testDuplexConnection;
  }
}
