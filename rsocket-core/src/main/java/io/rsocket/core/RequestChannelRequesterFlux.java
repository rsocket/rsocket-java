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
import io.rsocket.plugins.RequestInterceptor;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class RequestChannelRequesterFlux extends Flux<Payload>
    implements RequesterFrameHandler,
        LeaseHandler,
        CoreSubscriber<Payload>,
        Subscription,
        Scannable {

  final ByteBufAllocator allocator;
  final int mtu;
  final int maxFrameLength;
  final int maxInboundPayloadSize;
  final RequesterResponderSupport requesterResponderSupport;
  final DuplexConnection connection;
  final PayloadDecoder payloadDecoder;

  final Publisher<Payload> payloadsPublisher;

  @Nullable final RequesterLeaseTracker leaseTracker;
  @Nullable final RequestInterceptor requestInterceptor;

  volatile long state;
  static final AtomicLongFieldUpdater<RequestChannelRequesterFlux> STATE =
      AtomicLongFieldUpdater.newUpdater(RequestChannelRequesterFlux.class, "state");

  int streamId;

  Context cachedContext;

  boolean isFirstPayload = true;
  Payload firstPayload;

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
    this.leaseTracker = requesterResponderSupport.getRequesterLeaseTracker();
    this.requestInterceptor = requesterResponderSupport.getRequestInterceptor();
  }

  @Override
  public void subscribe(CoreSubscriber<? super Payload> actual) {
    Objects.requireNonNull(actual, "subscribe");

    long previousState = markSubscribed(STATE, this);
    if (isSubscribedOrTerminated(previousState)) {
      final IllegalStateException e =
          new IllegalStateException("RequestChannelFlux allows only a single Subscriber");
      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onReject(e, FrameType.REQUEST_CHANNEL, null);
      }

      Operators.error(actual, e);
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

    long previousState = addRequestN(STATE, this, n, leaseTracker == null);
    if (isTerminated(previousState)) {
      return;
    }

    if (hasRequested(previousState)) {
      if (isFirstFrameSent(previousState)
          && !isMaxAllowedRequestN(extractRequestN(previousState))) {
        final int streamId = this.streamId;
        final ByteBuf requestNFrame = RequestNFrameCodec.encode(this.allocator, streamId, n);
        this.connection.sendFrame(streamId, requestNFrame);
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

      final RequesterLeaseTracker leaseHandler = this.leaseTracker;
      final boolean leaseEnabled = leaseHandler != null;

      if (leaseEnabled) {
        this.firstPayload = p;

        final long previousState = markFirstPayloadReceived(STATE, this);
        if (isTerminated(previousState)) {
          this.firstPayload = null;
          p.release();
          return;
        }

        leaseHandler.issue(this);
      } else {
        final long state = this.state;
        if (isTerminated(state)) {
          p.release();
          return;
        }
        sendFirstPayload(p, extractRequestN(state));
      }
    } else {
      sendFollowingPayload(p);
    }
  }

  @Override
  public void handleLease() {
    final long previousState = markPrepared(STATE, this);
    if (isTerminated(previousState)) {
      return;
    }

    final Payload firstPayload = this.firstPayload;
    this.firstPayload = null;

    sendFirstPayload(firstPayload, extractRequestN(previousState));
  }

  void sendFirstPayload(Payload firstPayload, long initialRequestN) {
    int mtu = this.mtu;
    try {
      if (!isValid(mtu, this.maxFrameLength, firstPayload, true)) {
        lazyTerminate(STATE, this);

        this.outboundSubscription.cancel();

        final IllegalArgumentException e =
            new IllegalArgumentException(
                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength));
        final RequestInterceptor requestInterceptor = this.requestInterceptor;
        if (requestInterceptor != null) {
          requestInterceptor.onReject(e, FrameType.REQUEST_CHANNEL, firstPayload.metadata());
        }

        firstPayload.release();

        this.inboundDone = true;
        this.inboundSubscriber.onError(e);
        return;
      }
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);

      this.outboundSubscription.cancel();

      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onReject(e, FrameType.REQUEST_CHANNEL, null);
      }

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

      this.outboundSubscription.cancel();

      final Throwable ut = Exceptions.unwrap(t);
      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onReject(ut, FrameType.REQUEST_CHANNEL, firstPayload.metadata());
      }

      firstPayload.release();

      if (!isTerminated(previousState)) {
        this.inboundSubscriber.onError(ut);
      }
      return;
    }

    final RequestInterceptor requestInterceptor = this.requestInterceptor;
    if (requestInterceptor != null) {
      requestInterceptor.onStart(streamId, FrameType.REQUEST_CHANNEL, firstPayload.metadata());
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
    } catch (Throwable t) {
      lazyTerminate(STATE, this);

      sm.remove(streamId, this);
      this.outboundSubscription.cancel();

      this.inboundDone = true;

      if (requestInterceptor != null) {
        requestInterceptor.onTerminate(streamId, FrameType.REQUEST_CHANNEL, t);
      }

      this.inboundSubscriber.onError(t);
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
      connection.sendFrame(streamId, cancelFrame);

      if (requestInterceptor != null) {
        requestInterceptor.onCancel(streamId, FrameType.REQUEST_CHANNEL);
      }
      return;
    }

    if (isMaxAllowedRequestN(initialRequestN)) {
      return;
    }

    long requestN = extractRequestN(previousState);
    if (isMaxAllowedRequestN(requestN)) {
      final ByteBuf requestNFrame = RequestNFrameCodec.encode(allocator, streamId, requestN);
      connection.sendFrame(streamId, requestNFrame);
      return;
    }

    if (requestN > initialRequestN) {
      final ByteBuf requestNFrame =
          RequestNFrameCodec.encode(allocator, streamId, requestN - initialRequestN);
      connection.sendFrame(streamId, requestNFrame);
    }
  }

  final void sendFollowingPayload(Payload followingPayload) {
    int streamId = this.streamId;
    int mtu = this.mtu;

    try {
      if (!isValid(mtu, this.maxFrameLength, followingPayload, true)) {
        followingPayload.release();

        final IllegalArgumentException e =
            new IllegalArgumentException(
                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength));
        if (!this.tryCancel()) {
          Operators.onErrorDropped(e, this.inboundSubscriber.currentContext());
          return;
        }

        this.propagateErrorSafely(e);
        return;
      }
    } catch (IllegalReferenceCountException e) {
      if (!this.tryCancel()) {
        Operators.onErrorDropped(e, this.inboundSubscriber.currentContext());
        return;
      }

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
      if (!this.tryCancel()) {
        Operators.onErrorDropped(e, this.inboundSubscriber.currentContext());
        return;
      }

      this.propagateErrorSafely(e);
    }
  }

  void propagateErrorSafely(Throwable t) {
    // FIXME: must be scheduled on the connection event-loop to achieve serial
    //  behaviour on the inbound subscriber
    if (!this.inboundDone) {
      synchronized (this) {
        if (!this.inboundDone) {
          final RequestInterceptor interceptor = requestInterceptor;
          if (interceptor != null) {
            interceptor.onTerminate(this.streamId, FrameType.REQUEST_CHANNEL, t);
          }

          this.inboundDone = true;
          this.inboundSubscriber.onError(t);
        } else {
          Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
        }
      }
    } else {
      Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
    }
  }

  @Override
  public final void cancel() {
    if (!tryCancel()) {
      return;
    }

    final RequestInterceptor requestInterceptor = this.requestInterceptor;
    if (requestInterceptor != null) {
      requestInterceptor.onCancel(this.streamId, FrameType.REQUEST_CHANNEL);
    }
  }

  boolean tryCancel() {
    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      return false;
    }

    this.outboundSubscription.cancel();

    if (!isFirstFrameSent(previousState)) {
      if (!isPrepared(previousState) && isFirstPayloadReceived(previousState)) {
        final Payload firstPayload = this.firstPayload;
        this.firstPayload = null;
        firstPayload.release();
      }
      // no need to send anything, since we have not started a stream yet (no logical wire)
      return false;
    }

    final int streamId = this.streamId;
    this.requesterResponderSupport.remove(streamId, this);

    ReassemblyUtils.synchronizedRelease(this, previousState);

    final ByteBuf cancelFrame = CancelFrameCodec.encode(this.allocator, streamId);
    this.connection.sendFrame(streamId, cancelFrame);

    return true;
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
    this.connection.sendFrame(streamId, errorFrame);

    if (!isInboundTerminated(previousState)) {
      // FIXME: must be scheduled on the connection event-loop to achieve serial
      //  behaviour on the inbound subscriber
      synchronized (this) {
        final RequestInterceptor interceptor = requestInterceptor;
        if (interceptor != null) {
          interceptor.onTerminate(streamId, FrameType.REQUEST_CHANNEL, t);
        }

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

    final boolean isInboundTerminated = isInboundTerminated(previousState);
    if (isInboundTerminated) {
      this.requesterResponderSupport.remove(streamId, this);
    }

    final ByteBuf completeFrame = PayloadFrameCodec.encodeComplete(this.allocator, streamId);
    this.connection.sendFrame(streamId, completeFrame);

    if (isInboundTerminated) {
      final RequestInterceptor interceptor = requestInterceptor;
      if (interceptor != null) {
        interceptor.onTerminate(streamId, FrameType.REQUEST_CHANNEL, null);
      }
    }
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

      final RequestInterceptor interceptor = requestInterceptor;
      if (interceptor != null) {
        interceptor.onTerminate(streamId, FrameType.REQUEST_CHANNEL, null);
      }
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
    if (isTerminated(previousState)) {
      Operators.onErrorDropped(cause, this.inboundSubscriber.currentContext());
      return;
    } else if (isInboundTerminated(previousState)) {
      Operators.onErrorDropped(cause, this.inboundSubscriber.currentContext());
      return;
    }

    this.outboundSubscription.cancel();

    if (isPrepared(previousState)) {
      ReassemblyUtils.release(this, previousState);

      final int streamId = this.streamId;
      this.requesterResponderSupport.remove(streamId, this);

      final RequestInterceptor interceptor = requestInterceptor;
      if (interceptor != null) {
        interceptor.onTerminate(streamId, FrameType.REQUEST_CHANNEL, cause);
      }
    } else if (isFirstPayloadReceived(previousState)) {
      final Payload p = this.firstPayload;
      final RequestInterceptor interceptor = requestInterceptor;
      if (interceptor != null) {
        interceptor.onReject(cause, FrameType.REQUEST_CHANNEL, p.metadata());
      }
      p.release();
    }

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

    final boolean inboundTerminated = isInboundTerminated(previousState);
    if (inboundTerminated) {
      this.requesterResponderSupport.remove(this.streamId, this);
    }

    this.outboundSubscription.cancel();

    if (inboundTerminated) {
      final RequestInterceptor interceptor = requestInterceptor;
      if (interceptor != null) {
        interceptor.onTerminate(this.streamId, FrameType.REQUEST_CHANNEL, null);
      }
    }
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
