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
import static io.rsocket.core.ReassemblyUtils.handleNextSupport;
import static io.rsocket.core.SendUtils.DISCARD_CONTEXT;
import static io.rsocket.core.SendUtils.sendReleasingPayload;
import static io.rsocket.core.StateUtils.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.frame.CancelFrameCodec;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class RequestChannelRequesterFlux extends Flux<Payload>
    implements RequesterFrameHandler, CoreSubscriber<Payload>, Subscription, Scannable {

  final ByteBufAllocator allocator;
  final int mtu;
  final int maxFrameLength;
  final int maxInboundPayloadSize;
  final RequesterResponderSupport requesterResponderSupport;
  final DuplexConnection connection;
  final PayloadDecoder payloadDecoder;

  final Publisher<Payload> payloadsPublisher;

  volatile long state;
  static final AtomicLongFieldUpdater<RequestChannelRequesterFlux> STATE =
      AtomicLongFieldUpdater.newUpdater(RequestChannelRequesterFlux.class, "state");

  int streamId;

  Context cachedContext;

  boolean isFirstPayload = true;

  CoreSubscriber<? super Payload> inboundSubscriber;
  Subscription outboundSubscription;
  boolean inboundDone;
  boolean outboundDone;

  CompositeByteBuf frames;

  RequestChannelRequesterFlux(
      Publisher<Payload> payloadsPublisher, RequesterResponderSupport requesterResponderSupport) {
    this.allocator = requesterResponderSupport.getAllocator();
    this.payloadsPublisher = payloadsPublisher;
    this.mtu = requesterResponderSupport.getMtu();
    this.maxFrameLength = requesterResponderSupport.getMaxFrameLength();
    this.maxInboundPayloadSize = requesterResponderSupport.getMaxInboundPayloadSize();
    this.requesterResponderSupport = requesterResponderSupport;
    this.connection = requesterResponderSupport.getDuplexConnection();
    this.payloadDecoder = requesterResponderSupport.getPayloadDecoder();
  }

  @Override
  public void subscribe(CoreSubscriber<? super Payload> actual) {
    Objects.requireNonNull(actual, "subscribe");

    long previousState = markSubscribed(STATE, this);
    if (isSubscribedOrTerminated(previousState)) {
      Operators.error(
          actual, new IllegalStateException("RequestChannelFlux allows only a single Subscriber"));
      return;
    }

    this.inboundSubscriber = actual;
    this.payloadsPublisher.subscribe(this);
  }

  @Override
  public void onSubscribe(Subscription outboundSubscription) {
    if (Operators.validate(this.outboundSubscription, outboundSubscription)) {
      this.outboundSubscription = outboundSubscription;
      this.inboundSubscriber.onSubscribe(this);
    }
  }

  @Override
  public final void request(long n) {
    if (!Operators.validate(n)) {
      return;
    }

    long previousState = addRequestN(STATE, this, n);
    if (isTerminated(previousState)) {
      return;
    }

    if (hasRequested(previousState)) {
      if (isFirstFrameSent(previousState)
          && !isMaxAllowedRequestN(extractRequestN(previousState))) {
        final int streamId = this.streamId;
        final ByteBuf requestNFrame = RequestNFrameCodec.encode(this.allocator, streamId, n);
        this.connection.sendFrame(streamId, requestNFrame, false);
      }
      return;
    }

    // do first request
    this.outboundSubscription.request(1);
  }

  @Override
  public void onNext(Payload p) {
    if (this.outboundDone) {
      p.release();
      return;
    }

    if (this.isFirstPayload) {
      this.isFirstPayload = false;

      long state = this.state;
      if (isTerminated(state)) {
        p.release();
        return;
      }
      sendFirstPayload(p, extractRequestN(state));
    } else {
      sendFollowingPayload(p);
    }
  }

  void sendFirstPayload(Payload firstPayload, long initialRequestN) {
    int mtu = this.mtu;
    try {
      if (!isValid(mtu, this.maxFrameLength, firstPayload, true)) {
        lazyTerminate(STATE, this);

        firstPayload.release();
        this.outboundSubscription.cancel();

        this.inboundDone = true;
        this.inboundSubscriber.onError(
            new IllegalArgumentException(
                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength)));
        return;
      }
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);

      this.outboundSubscription.cancel();

      this.inboundDone = true;
      this.inboundSubscriber.onError(e);
      return;
    }

    final RequesterResponderSupport sm = this.requesterResponderSupport;
    final DuplexConnection connection = this.connection;
    final ByteBufAllocator allocator = this.allocator;

    final int streamId;
    try {
      streamId = sm.addAndGetNextStreamId(this);
      this.streamId = streamId;
    } catch (Throwable t) {
      this.inboundDone = true;
      final long previousState = markTerminated(STATE, this);

      firstPayload.release();
      this.outboundSubscription.cancel();

      if (!isTerminated(previousState)) {
        this.inboundSubscriber.onError(Exceptions.unwrap(t));
      }
      return;
    }

    try {
      sendReleasingPayload(
          streamId,
          FrameType.REQUEST_CHANNEL,
          initialRequestN,
          mtu,
          firstPayload,
          connection,
          allocator,
          // TODO: Should be a different flag in case of the scalar
          //  source or if we know in advance upstream is mono
          false);
    } catch (Throwable e) {
      lazyTerminate(STATE, this);

      sm.remove(streamId, this);
      this.outboundSubscription.cancel();

      this.inboundDone = true;
      this.inboundSubscriber.onError(e);
      return;
    }

    long previousState = markFirstFrameSent(STATE, this);
    if (isTerminated(previousState)) {
      if (this.inboundDone) {
        return;
      }

      sm.remove(streamId, this);

      ReassemblyUtils.synchronizedRelease(this, previousState);

      final ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, streamId);
      connection.sendFrame(streamId, cancelFrame, false);

      return;
    }

    if (isMaxAllowedRequestN(initialRequestN)) {
      return;
    }

    long requestN = extractRequestN(previousState);
    if (isMaxAllowedRequestN(requestN)) {
      final ByteBuf requestNFrame = RequestNFrameCodec.encode(allocator, streamId, requestN);
      connection.sendFrame(streamId, requestNFrame, false);
      return;
    }

    if (requestN > initialRequestN) {
      final ByteBuf requestNFrame =
          RequestNFrameCodec.encode(allocator, streamId, requestN - initialRequestN);
      connection.sendFrame(streamId, requestNFrame, false);
    }
  }

  final void sendFollowingPayload(Payload followingPayload) {
    int streamId = this.streamId;
    int mtu = this.mtu;

    try {
      if (!isValid(mtu, this.maxFrameLength, followingPayload, true)) {
        followingPayload.release();

        this.cancel();

        final IllegalArgumentException e =
            new IllegalArgumentException(
                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength));
        this.propagateErrorSafely(e);
        return;
      }
    } catch (IllegalReferenceCountException e) {
      this.cancel();

      this.propagateErrorSafely(e);

      return;
    }

    try {
      sendReleasingPayload(
          streamId,

          // TODO: Should be a different flag in case of the scalar
          //  source or if we know in advance upstream is mono
          FrameType.NEXT,
          mtu,
          followingPayload,
          this.connection,
          allocator,
          true);
    } catch (Throwable e) {
      this.cancel();

      this.propagateErrorSafely(e);
    }
  }

  void propagateErrorSafely(Throwable e) {
    // FIXME: must be scheduled on the connection event-loop to achieve serial
    //  behaviour on the inbound subscriber
    if (!this.inboundDone) {
      synchronized (this) {
        if (!this.inboundDone) {
          this.inboundDone = true;
          this.inboundSubscriber.onError(e);
        } else {
          Operators.onErrorDropped(e, this.inboundSubscriber.currentContext());
        }
      }
    } else {
      Operators.onErrorDropped(e, this.inboundSubscriber.currentContext());
    }
  }

  @Override
  public final void cancel() {
    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      return;
    }

    this.outboundSubscription.cancel();

    if (!isFirstFrameSent(previousState)) {
      // no need to send anything, since we have not started a stream yet (no logical wire)
      return;
    }

    final int streamId = this.streamId;
    this.requesterResponderSupport.remove(streamId, this);

    ReassemblyUtils.synchronizedRelease(this, previousState);

    final ByteBuf cancelFrame = CancelFrameCodec.encode(this.allocator, streamId);
    this.connection.sendFrame(streamId, cancelFrame, false);
  }

  @Override
  public void onError(Throwable t) {
    if (this.outboundDone) {
      Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
      return;
    }

    this.outboundDone = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState) || isOutboundTerminated(previousState)) {
      Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
      return;
    }

    if (!isFirstFrameSent(previousState)) {
      // first signal, thus, just propagates error to actual subscriber
      this.inboundSubscriber.onError(t);
      return;
    }

    ReassemblyUtils.synchronizedRelease(this, previousState);

    final int streamId = this.streamId;
    this.requesterResponderSupport.remove(streamId, this);
    // propagates error to remote responder
    final ByteBuf errorFrame = ErrorFrameCodec.encode(this.allocator, streamId, t);
    this.connection.sendFrame(streamId, errorFrame, false);

    if (!isInboundTerminated(previousState)) {
      // FIXME: must be scheduled on the connection event-loop to achieve serial
      //  behaviour on the inbound subscriber
      synchronized (this) {
        this.inboundDone = true;
        this.inboundSubscriber.onError(t);
      }
    } else {
      Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
    }
  }

  @Override
  public void onComplete() {
    if (this.outboundDone) {
      return;
    }

    this.outboundDone = true;

    long previousState = markOutboundTerminated(STATE, this, true);
    if (isTerminated(previousState) || isOutboundTerminated(previousState)) {
      return;
    }

    if (!isFirstFrameSent(previousState)) {
      // first signal, thus, just propagates error to actual subscriber
      this.inboundSubscriber.onError(new CancellationException("Empty Source"));
      return;
    }

    final int streamId = this.streamId;

    if (isInboundTerminated(previousState)) {
      this.requesterResponderSupport.remove(streamId, this);
    }

    final ByteBuf completeFrame = PayloadFrameCodec.encodeComplete(this.allocator, streamId);
    this.connection.sendFrame(streamId, completeFrame, false);
  }

  @Override
  public final void handleComplete() {
    if (this.inboundDone) {
      return;
    }

    this.inboundDone = true;

    long previousState = markInboundTerminated(STATE, this);
    if (isTerminated(previousState)) {
      return;
    }

    if (isOutboundTerminated(previousState)) {
      this.requesterResponderSupport.remove(this.streamId, this);
    }

    this.inboundSubscriber.onComplete();
  }

  @Override
  public final void handleError(Throwable cause) {
    if (this.inboundDone) {
      Operators.onErrorDropped(cause, this.inboundSubscriber.currentContext());
      return;
    }

    this.inboundDone = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState) || isInboundTerminated(previousState)) {
      Operators.onErrorDropped(cause, this.inboundSubscriber.currentContext());
      return;
    }

    ReassemblyUtils.release(this, previousState);

    final int streamId = this.streamId;

    this.requesterResponderSupport.remove(streamId, this);

    this.outboundSubscription.cancel();
    this.inboundSubscriber.onError(cause);
  }

  @Override
  public final void handlePayload(Payload value) {
    synchronized (this) {
      if (this.inboundDone) {
        value.release();
        return;
      }

      this.inboundSubscriber.onNext(value);
    }
  }

  @Override
  public void handleRequestN(long n) {
    this.outboundSubscription.request(n);
  }

  @Override
  public void handleCancel() {
    if (this.outboundDone) {
      return;
    }

    long previousState = markOutboundTerminated(STATE, this, false);
    if (isTerminated(previousState) || isOutboundTerminated(previousState)) {
      return;
    }

    if (isInboundTerminated(previousState)) {
      this.requesterResponderSupport.remove(this.streamId, this);
    }

    this.outboundSubscription.cancel();
  }

  @Override
  public void handleNext(ByteBuf frame, boolean hasFollows, boolean isLastPayload) {
    handleNextSupport(
        STATE,
        this,
        this,
        this.inboundSubscriber,
        this.payloadDecoder,
        this.allocator,
        this.maxInboundPayloadSize,
        frame,
        hasFollows,
        isLastPayload);
  }

  @Override
  @NonNull
  public Context currentContext() {
    long state = this.state;

    if (isSubscribedOrTerminated(state)) {
      Context contextWithDiscard = this.inboundSubscriber.currentContext().putAll(DISCARD_CONTEXT);
      cachedContext = contextWithDiscard;
      return contextWithDiscard;
    }

    return Context.empty();
  }

  @Override
  public CompositeByteBuf getFrames() {
    return this.frames;
  }

  @Override
  public void setFrames(CompositeByteBuf byteBuf) {
    this.frames = byteBuf;
  }

  @Override
  @Nullable
  public Object scanUnsafe(Attr key) {
    // touch guard
    long state = this.state;

    if (key == Attr.TERMINATED) return isTerminated(state);
    if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return state;

    return null;
  }

  @Override
  @NonNull
  public String stepName() {
    return "source(RequestChannelFlux)";
  }
}
