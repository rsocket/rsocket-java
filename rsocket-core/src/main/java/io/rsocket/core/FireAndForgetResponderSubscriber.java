/*
 * Copyright 2015-2020 the original author or authors.
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
package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.decoder.PayloadDecoder;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;

final class FireAndForgetResponderSubscriber
    implements CoreSubscriber<Void>, ResponderFrameHandler {

  static final Logger logger = LoggerFactory.getLogger(FireAndForgetResponderSubscriber.class);

  static final FireAndForgetResponderSubscriber INSTANCE = new FireAndForgetResponderSubscriber();

  final int streamId;
  final ByteBufAllocator allocator;
  final PayloadDecoder payloadDecoder;
  final RequesterResponderSupport requesterResponderSupport;
  final RSocket handler;
  final int maxInboundPayloadSize;

  CompositeByteBuf frames;

  private FireAndForgetResponderSubscriber() {
    this.streamId = 0;
    this.allocator = null;
    this.payloadDecoder = null;
    this.maxInboundPayloadSize = 0;
    this.requesterResponderSupport = null;
    this.handler = null;
    this.frames = null;
  }

  FireAndForgetResponderSubscriber(
      int streamId,
      ByteBuf firstFrame,
      RequesterResponderSupport requesterResponderSupport,
      RSocket handler) {
    this.streamId = streamId;
    this.allocator = requesterResponderSupport.getAllocator();
    this.payloadDecoder = requesterResponderSupport.getPayloadDecoder();
    this.maxInboundPayloadSize = requesterResponderSupport.getMaxInboundPayloadSize();
    this.requesterResponderSupport = requesterResponderSupport;
    this.handler = handler;

    this.frames =
        ReassemblyUtils.addFollowingFrame(
            allocator.compositeBuffer(), firstFrame, maxInboundPayloadSize);
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(Void voidVal) {}

  @Override
  public void onError(Throwable t) {
    logger.debug("Dropped Outbound error", t);
  }

  @Override
  public void onComplete() {}

  @Override
  public void handleNext(ByteBuf followingFrame, boolean hasFollows, boolean isLastPayload) {
    final CompositeByteBuf frames;
    try {
      frames =
          ReassemblyUtils.addFollowingFrame(
              this.frames, followingFrame, this.maxInboundPayloadSize);
    } catch (IllegalStateException t) {
      this.requesterResponderSupport.remove(this.streamId, this);

      CompositeByteBuf framesToRelease = this.frames;
      this.frames = null;
      framesToRelease.release();

      logger.debug("Reassembly has failed", t);
      return;
    }

    if (!hasFollows) {
      this.requesterResponderSupport.remove(this.streamId, this);
      this.frames = null;

      Payload payload;
      try {
        payload = this.payloadDecoder.apply(frames);
        frames.release();
      } catch (Throwable t) {
        ReferenceCountUtil.safeRelease(frames);
        logger.debug("Reassembly has failed", t);
        return;
      }

      Mono<Void> source = this.handler.fireAndForget(payload);
      source.subscribe(this);
    }
  }

  @Override
  public final void handleCancel() {
    final CompositeByteBuf frames = this.frames;
    if (frames != null) {
      this.frames = null;
      this.requesterResponderSupport.remove(this.streamId, this);
      frames.release();
    }
  }
}
