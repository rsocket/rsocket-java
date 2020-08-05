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

import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;
import static io.rsocket.core.PayloadValidationUtils.isValid;
import static io.rsocket.core.SendUtils.sendReleasingPayload;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.CanceledException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class RequestResponseResponderSubscriber
    implements ResponderFrameHandler, CoreSubscriber<Payload> {

  static final Logger logger = LoggerFactory.getLogger(RequestResponseResponderSubscriber.class);

  final int streamId;
  final ByteBufAllocator allocator;
  final PayloadDecoder payloadDecoder;
  final int mtu;
  final int maxFrameLength;
  final int maxInboundPayloadSize;
  final RequesterResponderSupport requesterResponderSupport;
  final UnboundedProcessor<ByteBuf> sendProcessor;

  final RSocket handler;

  CompositeByteBuf frames;

  volatile Subscription s;
  static final AtomicReferenceFieldUpdater<RequestResponseResponderSubscriber, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(
          RequestResponseResponderSubscriber.class, Subscription.class, "s");

  public RequestResponseResponderSubscriber(
      int streamId,
      ByteBuf firstFrame,
      RequesterResponderSupport requesterResponderSupport,
      RSocket handler) {
    this.streamId = streamId;
    this.allocator = requesterResponderSupport.getAllocator();
    this.mtu = requesterResponderSupport.getMtu();
    this.maxFrameLength = requesterResponderSupport.getMaxFrameLength();
    this.maxInboundPayloadSize = requesterResponderSupport.getMaxInboundPayloadSize();
    this.requesterResponderSupport = requesterResponderSupport;
    this.sendProcessor = requesterResponderSupport.getSendProcessor();
    this.payloadDecoder = requesterResponderSupport.getPayloadDecoder();
    this.handler = handler;
    this.frames =
        ReassemblyUtils.addFollowingFrame(
            allocator.compositeBuffer(), firstFrame, maxInboundPayloadSize);
  }

  public RequestResponseResponderSubscriber(
      int streamId, RequesterResponderSupport requesterResponderSupport) {
    this.streamId = streamId;
    this.allocator = requesterResponderSupport.getAllocator();
    this.mtu = requesterResponderSupport.getMtu();
    this.maxFrameLength = requesterResponderSupport.getMaxFrameLength();
    this.maxInboundPayloadSize = requesterResponderSupport.getMaxInboundPayloadSize();
    this.requesterResponderSupport = requesterResponderSupport;
    this.sendProcessor = requesterResponderSupport.getSendProcessor();

    this.payloadDecoder = null;
    this.handler = null;
    this.frames = null;
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    if (Operators.validate(this.s, subscription)) {
      S.lazySet(this, subscription);
      subscription.request(Long.MAX_VALUE);
    }
  }

  @Override
  public void onNext(@Nullable Payload p) {
    if (!Operators.terminate(S, this)) {
      if (p != null) {
        p.release();
      }
      return;
    }

    final int streamId = this.streamId;
    final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
    final ByteBufAllocator allocator = this.allocator;

    this.requesterResponderSupport.remove(streamId, this);

    if (p == null) {
      final ByteBuf completeFrame = PayloadFrameCodec.encodeComplete(allocator, streamId);
      sender.onNext(completeFrame);
      return;
    }

    final int mtu = this.mtu;
    try {
      if (!isValid(mtu, this.maxFrameLength, p, false)) {
        p.release();

        final ByteBuf errorFrame =
            ErrorFrameCodec.encode(
                allocator,
                streamId,
                new CanceledException(
                    String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength)));
        sender.onNext(errorFrame);
        return;
      }
    } catch (IllegalReferenceCountException e) {
      final ByteBuf errorFrame =
          ErrorFrameCodec.encode(
              allocator,
              streamId,
              new CanceledException("Failed to validate payload. Cause" + e.getMessage()));
      sender.onNext(errorFrame);
      return;
    }

    try {
      sendReleasingPayload(streamId, FrameType.NEXT_COMPLETE, mtu, p, sender, allocator, false);
    } catch (Throwable ignored) {
    }
  }

  @Override
  public void onError(Throwable t) {
    if (S.getAndSet(this, Operators.cancelledSubscription()) == Operators.cancelledSubscription()) {
      logger.debug("Dropped error", t);
      return;
    }

    final int streamId = this.streamId;

    this.requesterResponderSupport.remove(streamId, this);

    final ByteBuf errorFrame = ErrorFrameCodec.encode(this.allocator, streamId, t);
    this.sendProcessor.onNext(errorFrame);
  }

  @Override
  public void onComplete() {
    onNext(null);
  }

  @Override
  public void handleCancel() {
    final Subscription currentSubscription = this.s;
    if (currentSubscription == Operators.cancelledSubscription()) {
      return;
    }

    if (currentSubscription == null) {
      // if subscription is null, it means that streams has not yet reassembled all the fragments
      // and fragmentation of the first frame was cancelled before
      S.lazySet(this, Operators.cancelledSubscription());

      this.requesterResponderSupport.remove(this.streamId, this);

      final CompositeByteBuf frames = this.frames;
      this.frames = null;
      frames.release();

      return;
    }

    if (!S.compareAndSet(this, currentSubscription, Operators.cancelledSubscription())) {
      return;
    }

    this.requesterResponderSupport.remove(this.streamId, this);

    currentSubscription.cancel();
  }

  @Override
  public void handleNext(ByteBuf frame, boolean hasFollows, boolean isLastPayload) {
    final CompositeByteBuf frames;
    try {
      frames = ReassemblyUtils.addFollowingFrame(this.frames, frame, this.maxInboundPayloadSize);
    } catch (IllegalStateException t) {
      S.lazySet(this, Operators.cancelledSubscription());

      this.requesterResponderSupport.remove(this.streamId, this);

      CompositeByteBuf framesToRelease = this.frames;
      this.frames = null;
      framesToRelease.release();

      logger.debug("Reassembly has failed", t);

      // sends error frame from the responder side to tell that something went wrong
      final ByteBuf errorFrame =
          ErrorFrameCodec.encode(
              this.allocator,
              this.streamId,
              new CanceledException("Failed to reassemble payload. Cause: " + t.getMessage()));
      this.sendProcessor.onNext(errorFrame);
      return;
    }

    if (!hasFollows) {
      this.frames = null;
      Payload payload;
      try {
        payload = this.payloadDecoder.apply(frames);
        frames.release();
      } catch (Throwable t) {
        S.lazySet(this, Operators.cancelledSubscription());

        this.requesterResponderSupport.remove(this.streamId, this);

        ReferenceCountUtil.safeRelease(frames);

        logger.debug("Reassembly has failed", t);

        // sends error frame from the responder side to tell that something went wrong
        final ByteBuf errorFrame =
            ErrorFrameCodec.encode(
                this.allocator,
                this.streamId,
                new CanceledException("Failed to reassemble payload. Cause: " + t.getMessage()));
        this.sendProcessor.onNext(errorFrame);
        return;
      }

      final Mono<Payload> source = this.handler.requestResponse(payload);
      source.subscribe(this);
    }
  }

  @Override
  public Context currentContext() {
    return SendUtils.DISCARD_CONTEXT;
  }
}
