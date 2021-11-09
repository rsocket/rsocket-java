/*
 * Copyright 2015-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.transport.aeron;

import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.internal.BaseDuplexConnection;
import java.net.SocketAddress;
import reactor.core.publisher.Flux;

class AeronDuplexConnection extends BaseDuplexConnection implements AutoCloseable {

  final int streamId;
  final FluxReceiver fluxReceiver;
  final MonoSendMany monoSendMany;
  final ExclusivePublication publication;
  final Subscription subscription;
  final ByteBufAllocator allocator;
  final AeronChannelAddress aeronChannelAddress;

  public AeronDuplexConnection(
      int streamId,
      ExclusivePublication publication,
      Subscription subscription,
      EventLoopGroup eventLoopGroup,
      ByteBufAllocator allocator,
      int prefetch,
      int effort) {
    this.streamId = streamId;
    this.aeronChannelAddress = new AeronChannelAddress(subscription.channel());
    this.publication = publication;
    this.subscription = subscription;
    this.allocator = allocator;

    this.fluxReceiver = new FluxReceiver(this.onClose, eventLoopGroup.next(), subscription, effort);
    this.monoSendMany =
        this.sender.subscribeWith(
            new MonoSendMany(this.onClose, eventLoopGroup.next(), publication, effort, prefetch));
  }

  public int streamId() {
    return this.streamId;
  }

  @Override
  public void sendErrorAndClose(RSocketErrorException e) {
    final ByteBuf errorFrame = ErrorFrameCodec.encode(this.allocator, 0, e);
    this.sender.onNext(errorFrame);

    final Throwable cause = e.getCause();
    if (cause == null) {
      this.sender.onComplete();
    } else {
      this.sender.onError(cause);
    }
  }

  @Override
  public Flux<ByteBuf> receive() {
    return this.fluxReceiver;
  }

  @Override
  public ByteBufAllocator alloc() {
    return this.allocator;
  }

  @Override
  public SocketAddress remoteAddress() {
    return this.aeronChannelAddress;
  }

  @Override
  protected void doOnClose() {
    this.fluxReceiver.dispose();
    this.monoSendMany.dispose();
  }

  @Override
  public void close() {
    dispose();
  }
}
