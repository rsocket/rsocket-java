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
import static io.rsocket.core.SendUtils.sendReleasingPayload;
import static io.rsocket.core.StateUtils.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.frame.CancelFrameCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.plugins.RequestInterceptor;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

final class RequestStreamRequesterFlux extends Flux<Payload>
    implements RequesterFrameHandler, LeaseHandler, Subscription, Scannable {

  final ByteBufAllocator allocator;
  final Payload payload;
  final int mtu;
  final int maxFrameLength;
  final int maxInboundPayloadSize;
  final RequesterResponderSupport requesterResponderSupport;
  final DuplexConnection connection;
  final PayloadDecoder payloadDecoder;

  @Nullable final RequesterLeaseTracker leaseTracker;
  @Nullable final RequestInterceptor requestInterceptor;

  volatile long state;
  static final AtomicLongFieldUpdater<RequestStreamRequesterFlux> STATE =
      AtomicLongFieldUpdater.newUpdater(RequestStreamRequesterFlux.class, "state");

  int streamId;
  CoreSubscriber<? super Payload> inboundSubscriber;
  CompositeByteBuf frames;
  boolean done;

  RequestStreamRequesterFlux(Payload payload, RequesterResponderSupport requesterResponderSupport) {
    this.allocator = requesterResponderSupport.getAllocator();
    this.payload = payload;
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
    long previousState = markSubscribed(STATE, this);
    if (isSubscribedOrTerminated(previousState)) {
      final IllegalStateException e =
          new IllegalStateException("RequestStreamFlux allows only a single Subscriber");
      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onReject(e, FrameType.REQUEST_STREAM, null);
      }

      Operators.error(actual, e);
      return;
    }

    final Payload p = this.payload;
    try {
      if (!isValid(this.mtu, this.maxFrameLength, p, false)) {
        lazyTerminate(STATE, this);

        final IllegalArgumentException e =
            new IllegalArgumentException(
                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength));
        final RequestInterceptor requestInterceptor = this.requestInterceptor;
        if (requestInterceptor != null) {
          requestInterceptor.onReject(e, FrameType.REQUEST_STREAM, p.metadata());
        }

        p.release();

        Operators.error(actual, e);
        return;
      }
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);

      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onReject(e, FrameType.REQUEST_STREAM, null);
      }

      Operators.error(actual, e);
      return;
    }

    this.inboundSubscriber = actual;
    actual.onSubscribe(this);
  }

  @Override
  public final void request(long n) {
    if (!Operators.validate(n)) {
      return;
    }

    final RequesterLeaseTracker leaseHandler = this.leaseTracker;
    final boolean leaseEnabled = leaseHandler != null;
    final long previousState = addRequestN(STATE, this, n, !leaseEnabled);
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

    if (leaseEnabled) {
      leaseHandler.issue(this);
      return;
    }

    sendFirstPayload(this.payload, n);
  }

  @Override
  public void handleLease() {
    final long previousState = markPrepared(STATE, this);

    if (isTerminated(previousState)) {
      return;
    }

    sendFirstPayload(this.payload, extractRequestN(previousState));
  }

  void sendFirstPayload(Payload payload, long initialRequestN) {

    final RequesterResponderSupport sm = this.requesterResponderSupport;
    final DuplexConnection connection = this.connection;
    final ByteBufAllocator allocator = this.allocator;

    final int streamId;
    try {
      streamId = sm.addAndGetNextStreamId(this);
      this.streamId = streamId;
    } catch (Throwable t) {
      this.done = true;
      final long previousState = markTerminated(STATE, this);

      final Throwable ut = Exceptions.unwrap(t);
      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onReject(ut, FrameType.REQUEST_STREAM, payload.metadata());
      }

      payload.release();

      if (!isTerminated(previousState)) {
        this.inboundSubscriber.onError(ut);
      }
      return;
    }

    final RequestInterceptor requestInterceptor = this.requestInterceptor;
    if (requestInterceptor != null) {
      requestInterceptor.onStart(streamId, FrameType.REQUEST_STREAM, payload.metadata());
    }

    try {
      sendReleasingPayload(
          streamId,
          FrameType.REQUEST_STREAM,
          initialRequestN,
          this.mtu,
          payload,
          connection,
          allocator,
          false);
    } catch (Throwable t) {
      this.done = true;
      lazyTerminate(STATE, this);

      sm.remove(streamId, this);

      if (requestInterceptor != null) {
        requestInterceptor.onTerminate(streamId, FrameType.REQUEST_STREAM, t);
      }

      this.inboundSubscriber.onError(t);
      return;
    }

    long previousState = markFirstFrameSent(STATE, this);
    if (isTerminated(previousState)) {
      if (this.done) {
        return;
      }

      sm.remove(streamId, this);

      final ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, streamId);
      connection.sendFrame(streamId, cancelFrame);

      if (requestInterceptor != null) {
        requestInterceptor.onCancel(streamId, FrameType.REQUEST_STREAM);
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

  @Override
  public final void cancel() {
    final long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      return;
    }

    if (isFirstFrameSent(previousState)) {
      final int streamId = this.streamId;
      this.requesterResponderSupport.remove(streamId, this);

      ReassemblyUtils.synchronizedRelease(this, previousState);

      this.connection.sendFrame(streamId, CancelFrameCodec.encode(this.allocator, streamId));

      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onCancel(streamId, FrameType.REQUEST_STREAM);
      }
    } else if (!isPrepared(previousState)) {
      // no need to send anything, since the first request has not happened
      this.payload.release();
    }
  }

  @Override
  public final void handlePayload(Payload p) {
    if (this.done) {
      p.release();
      return;
    }

    this.inboundSubscriber.onNext(p);
  }

  @Override
  public final void handleComplete() {
    if (this.done) {
      return;
    }

    this.done = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      return;
    }

    final int streamId = this.streamId;
    this.requesterResponderSupport.remove(streamId, this);

    final RequestInterceptor requestInterceptor = this.requestInterceptor;
    if (requestInterceptor != null) {
      requestInterceptor.onTerminate(streamId, FrameType.REQUEST_STREAM, null);
    }

    this.inboundSubscriber.onComplete();
  }

  @Override
  public final void handleError(Throwable cause) {
    if (this.done) {
      Operators.onErrorDropped(cause, this.inboundSubscriber.currentContext());
      return;
    }

    this.done = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      Operators.onErrorDropped(cause, this.inboundSubscriber.currentContext());
      return;
    }

    if (isPrepared(previousState)) {
      final int streamId = this.streamId;
      this.requesterResponderSupport.remove(streamId, this);

      ReassemblyUtils.synchronizedRelease(this, previousState);

      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onTerminate(streamId, FrameType.REQUEST_STREAM, cause);
      }
    } else {
      final Payload p = this.payload;
      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onReject(cause, FrameType.REQUEST_STREAM, p.metadata());
      }
      p.release();
    }

    this.inboundSubscriber.onError(cause);
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
    if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return extractRequestN(state);

    return null;
  }

  @Override
  @NonNull
  public String stepName() {
    return "source(RequestStreamFlux)";
  }
}
