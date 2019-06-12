package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import javax.annotation.Nullable;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Function;

public interface LeaseSender {

  void sendLease(Function<Optional<LeaseStats>, Flux<Lease>> )

  void sendLease(int timeToLiveMillis, int numberOfRequests, @Nullable ByteBuf metadata);

  default void sendLease(int timeToLiveMillis, int numberOfRequests) {
    sendLease(timeToLiveMillis, numberOfRequests, Unpooled.EMPTY_BUFFER);
  }

  Lease responderLease();

  Mono<Void> onClose();

  boolean isDisposed();
}
