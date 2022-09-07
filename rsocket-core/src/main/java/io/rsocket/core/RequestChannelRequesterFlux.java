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
import reactor.util.context.ContextView;

final class RequestChannelRequesterFlux extends Flux<Payload>
    implements RequesterFrameHandler,
        LeasePermitHandler,
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

  @Nullable final RequesterLeaseTracker requesterLeaseTracker;
  @Nullable final RequestInterceptor requestInterceptor;

  volatile long state;
  static final AtomicLongFieldUpdater<RequestChannelRequesterFlux> STATE =
      AtomicLongFieldUpdater.newUpdater(RequestChannelRequesterFlux.class, "state");

  int streamId;

  boolean isFirstSignal = true;
  Payload firstPayload;

  Subscription outboundSubscription;
  boolean outboundDone;
  Throwable outboundError;

  Context cachedContext;
  CoreSubscriber<? super Payload> inboundSubscriber;
  boolean inboundDone;
  long requested;
  long produced;

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
    this.requesterLeaseTracker = requesterResponderSupport.getRequesterLeaseTracker();
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

    this.requested = Operators.addCap(this.requested, n);

    long previousState = addRequestN(STATE, this, n, this.requesterLeaseTracker == null);
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

    if (this.isFirstSignal) {
      this.isFirstSignal = false;

      final RequesterLeaseTracker requesterLeaseTracker = this.requesterLeaseTracker;
      final boolean leaseEnabled = requesterLeaseTracker != null;

      if (leaseEnabled) {
        this.firstPayload = p;

        final long previousState = markFirstPayloadReceived(STATE, this);
        if (isTerminated(previousState)) {
          this.firstPayload = null;
          p.release();
          return;
        }

        requesterLeaseTracker.issue(this);
      } else {
        final long state = this.state;
        if (isTerminated(state)) {
          p.release();
          return;
        }
        // TODO: check if source is Scalar | Callable | Mono
        sendFirstPayload(p, extractRequestN(state), false);
      }
    } else {
      sendFollowingPayload(p);
    }
  }

  @Override
  public boolean handlePermit() {
    final long previousState = markReadyToSendFirstFrame(STATE, this);

    if (isTerminated(previousState)) {
      return false;
    }

    final Payload firstPayload = this.firstPayload;
    this.firstPayload = null;

    sendFirstPayload(
        firstPayload, extractRequestN(previousState), isOutboundTerminated(previousState));
    return true;
  }

  void sendFirstPayload(Payload firstPayload, long initialRequestN, boolean completed) {
    int mtu = this.mtu;
    try {
      if (!isValid(mtu, this.maxFrameLength, firstPayload, true)) {
        final long previousState = markTerminated(STATE, this);

        if (isTerminated(previousState)) {
          return;
        }

        if (!isOutboundTerminated(previousState)) {
          this.outboundSubscription.cancel();
        }

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
      final long previousState = markTerminated(STATE, this);

      if (isTerminated(previousState)) {
        Operators.onErrorDropped(e, this.inboundSubscriber.currentContext());
        return;
      }

      if (!isOutboundTerminated(previousState)) {
        this.outboundSubscription.cancel();
      }

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
      final long previousState = markTerminated(STATE, this);

      firstPayload.release();

      if (isTerminated(previousState)) {
        Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
        return;
      }

      if (!isOutboundTerminated(previousState)) {
        this.outboundSubscription.cancel();
      }

      final Throwable ut = Exceptions.unwrap(t);
      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onReject(ut, FrameType.REQUEST_CHANNEL, firstPayload.metadata());
      }

      this.inboundDone = true;
      this.inboundSubscriber.onError(ut);

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
          completed);
    } catch (Throwable t) {
      final long previousState = markTerminated(STATE, this);

      firstPayload.release();

      if (isTerminated(previousState)) {
        Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
        return;
      }

      sm.remove(streamId, this);

      if (!isOutboundTerminated(previousState)) {
        this.outboundSubscription.cancel();
      }

      if (requestInterceptor != null) {
        requestInterceptor.onTerminate(streamId, FrameType.REQUEST_CHANNEL, t);
      }

      this.inboundDone = true;
      this.inboundSubscriber.onError(t);
      return;
    }

    long previousState = markFirstFrameSent(STATE, this);
    if (isTerminated(previousState)) {
      // now, this can be terminated in case of the following scenarios:
      //
      // 1) SendFirst is called synchronously from onNext, thus we can have
      //    handleError called before we marked first frame sent, thus we may check if
      //    inboundDone flag is true and exit execution without any further actions:
      if (this.inboundDone) {
        return;
      }

      sm.remove(streamId, this);

      // 2) SendFirst is called asynchronously on the connection event-loop. Thus, we
      // need to check if outbound error is present. Note, we check outboundError since
      // in the last scenario, cancellation may terminate the state and async
      // onComplete may set outboundDone to true. Thus, we explicitly check for
      // outboundError
      final Throwable outboundError = this.outboundError;
      if (outboundError != null) {
        final ByteBuf errorFrame = ErrorFrameCodec.encode(allocator, streamId, outboundError);
        connection.sendFrame(streamId, errorFrame);

        if (requestInterceptor != null) {
          requestInterceptor.onTerminate(streamId, FrameType.REQUEST_CHANNEL, outboundError);
        }

        this.inboundDone = true;
        this.inboundSubscriber.onError(outboundError);
      } else {
        // 3) SendFirst is interleaving with cancel. Thus, we need to generate cancel
        // frame
        final ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, streamId);
        connection.sendFrame(streamId, cancelFrame);

        if (requestInterceptor != null) {
          requestInterceptor.onCancel(streamId, FrameType.REQUEST_CHANNEL);
        }
      }

      return;
    }

    if (!completed && isOutboundTerminated(previousState)) {
      final ByteBuf completeFrame = PayloadFrameCodec.encodeComplete(this.allocator, streamId);
      connection.sendFrame(streamId, completeFrame);
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

    if (!isOutboundTerminated(previousState)) {
      this.outboundSubscription.cancel();
    }

    if (!isReadyToSendFirstFrame(previousState) && isFirstPayloadReceived(previousState)) {
      final Payload firstPayload = this.firstPayload;
      this.firstPayload = null;
      firstPayload.release();
      // no need to send anything, since we have not started a stream yet (no logical wire)
      return false;
    }

    ReassemblyUtils.synchronizedRelease(this, previousState);

    final boolean firstFrameSent = isFirstFrameSent(previousState);
    if (firstFrameSent) {
      final int streamId = this.streamId;
      this.requesterResponderSupport.remove(streamId, this);

      final ByteBuf cancelFrame = CancelFrameCodec.encode(this.allocator, streamId);
      this.connection.sendFrame(streamId, cancelFrame);
    }

    return firstFrameSent;
  }

  @Override
  public void onError(Throwable t) {
    if (this.outboundDone) {
      Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
      return;
    }

    this.outboundError = t;
    this.outboundDone = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState) || isOutboundTerminated(previousState)) {
      Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
      return;
    }

    if (this.isFirstSignal) {
      this.inboundDone = true;
      this.inboundSubscriber.onError(t);
      return;
    } else if (!isReadyToSendFirstFrame(previousState)) {
      // first signal is received but we are still waiting for lease permit to be issued,
      // thus, just propagates error to actual subscriber

      final Payload firstPayload = this.firstPayload;
      this.firstPayload = null;

      firstPayload.release();

      this.inboundDone = true;
      this.inboundSubscriber.onError(t);

      return;
    }

    ReassemblyUtils.synchronizedRelease(this, previousState);

    if (isFirstFrameSent(previousState)) {
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
      if (!isFirstPayloadReceived(previousState)) {
        // first signal, thus, just propagates error to actual subscriber
        this.inboundSubscriber.onError(new CancellationException("Empty Source"));
      }
      return;
    }

    final int streamId = this.streamId;
    final ByteBuf completeFrame = PayloadFrameCodec.encodeComplete(this.allocator, streamId);

    this.connection.sendFrame(streamId, completeFrame);

    if (isInboundTerminated(previousState)) {
      this.requesterResponderSupport.remove(streamId, this);

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

      final RequestInterceptor interceptor = this.requestInterceptor;
      if (interceptor != null) {
        interceptor.onTerminate(this.streamId, FrameType.REQUEST_CHANNEL, null);
      }
    }

    this.inboundSubscriber.onComplete();
  }

  @Override
  public final void handlePermitError(Throwable cause) {
    this.inboundDone = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState) || isInboundTerminated(previousState)) {
      Operators.onErrorDropped(cause, this.inboundSubscriber.currentContext());
      return;
    }

    if (!isOutboundTerminated(previousState)) {
      this.outboundSubscription.cancel();
    }

    final Payload p = this.firstPayload;
    final RequestInterceptor interceptor = requestInterceptor;
    if (interceptor != null) {
      interceptor.onReject(cause, FrameType.REQUEST_CHANNEL, p.metadata());
    }
    p.release();

    this.inboundSubscriber.onError(cause);
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

    if (!isOutboundTerminated(previousState)) {
      this.outboundSubscription.cancel();
    }

    ReassemblyUtils.release(this, previousState);

    final int streamId = this.streamId;
    this.requesterResponderSupport.remove(streamId, this);

    final RequestInterceptor interceptor = requestInterceptor;
    if (interceptor != null) {
      interceptor.onTerminate(streamId, FrameType.REQUEST_CHANNEL, cause);
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

      final long produced = this.produced;
      if (this.requested == produced) {
        value.release();
        if (!tryCancel()) {
          return;
        }

        final Throwable cause =
            Exceptions.failWithOverflow(
                "The number of messages received exceeds the number requested");
        final RequestInterceptor requestInterceptor = this.requestInterceptor;
        if (requestInterceptor != null) {
          requestInterceptor.onTerminate(streamId, FrameType.REQUEST_CHANNEL, cause);
        }

        this.inboundSubscriber.onError(cause);
        return;
      }

      this.produced = produced + 1;

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
      Context cachedContext = this.cachedContext;
      if (cachedContext == null) {
        cachedContext =
            this.inboundSubscriber.currentContext().putAll((ContextView) DISCARD_CONTEXT);
        this.cachedContext = cachedContext;
      }
      return cachedContext;
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
