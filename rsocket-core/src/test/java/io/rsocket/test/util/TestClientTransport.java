package io.rsocket.test.util;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.transport.ClientTransport;
import reactor.core.publisher.Mono;

public class TestClientTransport implements ClientTransport {
  private final LeaksTrackingByteBufAllocator allocator =
      LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
  private final TestDuplexConnection testDuplexConnection = new TestDuplexConnection(allocator);

  @Override
  public Mono<DuplexConnection> connect(int mtu) {
    return Mono.just(testDuplexConnection);
  }

  public TestDuplexConnection testConnection() {
    return testDuplexConnection;
  }

  public LeaksTrackingByteBufAllocator alloc() {
    return allocator;
  }
}
