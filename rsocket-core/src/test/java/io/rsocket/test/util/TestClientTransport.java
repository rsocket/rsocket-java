package io.rsocket.test.util;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.transport.ClientTransport;
import reactor.core.publisher.Mono;

public class TestClientTransport implements ClientTransport {
  private final LeaksTrackingByteBufAllocator allocator =
      LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
  private final TestDuplexConnection testDuplexConnection = new TestDuplexConnection(allocator);

  int maxFrameLength = FRAME_LENGTH_MASK;

  @Override
  public Mono<DuplexConnection> connect() {
    return Mono.just(testDuplexConnection);
  }

  public TestDuplexConnection testConnection() {
    return testDuplexConnection;
  }

  public LeaksTrackingByteBufAllocator alloc() {
    return allocator;
  }

  public TestClientTransport withMaxFrameLength(int maxFrameLength) {
    this.maxFrameLength = maxFrameLength;
    return this;
  }

  @Override
  public int maxFrameLength() {
    return maxFrameLength;
  }
}
