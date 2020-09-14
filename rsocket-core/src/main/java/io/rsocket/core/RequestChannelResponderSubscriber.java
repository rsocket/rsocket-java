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
import static io.rsocket.core.StateUtils.*;
import static reactor.core.Exceptions.TERMINATED;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.CanceledException;
import io.rsocket.frame.CancelFrameCodec;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

final class RequestChannelResponderSubscriber extends Flux<Payload>
    implements ResponderFrameHandler, Subscription, CoreSubscriber<Payload> {

  static final Logger logger = LoggerFactory.getLogger(RequestChannelResponderSubscriber.class);

  final int streamId;
  final ByteBufAllocator allocator;
  final PayloadDecoder payloadDecoder;
  final int mtu;
  final int maxFrameLength;
  final int maxInboundPayloadSize;
  final RequesterResponderSupport requesterResponderSupport;
  final DuplexConnection connection;
  final long firstRequest;

  final RSocket handler;

  volatile long state;
  static final AtomicLongFieldUpdater<RequestChannelResponderSubscriber> STATE =
      AtomicLongFieldUpdater.newUpdater(RequestChannelResponderSubscriber.class, "state");

  Payload firstPayload;

  Subscription outboundSubscription;
  CoreSubscriber<? super Payload> inboundSubscriber;

  CompositeByteBuf frames;

  volatile Throwable inboundError;
  static final AtomicReferenceFieldUpdater<RequestChannelResponderSubscriber, Throwable>
      INBOUND_ERROR =
          AtomicReferenceFieldUpdater.newUpdater(
              RequestChannelResponderSubscriber.class, Throwable.class, "inboundError");

  boolean inboundDone;
  boolean outboundDone;

  public RequestChannelResponderSubscriber(
      int streamId,
      long firstRequestN,
      ByteBuf firstFrame,
      RequesterResponderSupport requesterResponderSupport,
      RSocket handler) {
    this.streamId = streamId;
    this.allocator = requesterResponderSupport.getAllocator();
    this.mtu = requesterResponderSupport.getMtu();
    this.maxFrameLength = requesterResponderSupport.getMaxFrameLength();
    this.maxInboundPayloadSize = requesterResponderSupport.getMaxInboundPayloadSize();
    this.requesterResponderSupport = requesterResponderSupport;
    this.connection = requesterResponderSupport.getDuplexConnection();
    this.payloadDecoder = requesterResponderSupport.getPayloadDecoder();
    this.handler = handler;
    this.firstRequest = firstRequestN;

    this.frames =
        ReassemblyUtils.addFollowingFrame(
            allocator.compositeBuffer(), firstFrame, true, maxInboundPayloadSize);
    STATE.lazySet(this, REASSEMBLING_FLAG);
  }

  public RequestChannelResponderSubscriber(
      int streamId,
      long firstRequestN,
      Payload firstPayload,
      RequesterResponderSupport requesterResponderSupport) {
    this.streamId = streamId;
    this.allocator = requesterResponderSupport.getAllocator();
    this.mtu = requesterResponderSupport.getMtu();
    this.maxFrameLength = requesterResponderSupport.getMaxFrameLength();
    this.maxInboundPayloadSize = requesterResponderSupport.getMaxInboundPayloadSize();
    this.requesterResponderSupport = requesterResponderSupport;
    this.connection = requesterResponderSupport.getDuplexConnection();
    this.payloadDecoder = requesterResponderSupport.getPayloadDecoder();
    this.firstRequest = firstRequestN;
    this.firstPayload = firstPayload;

    this.handler = null;
    this.frames = null;
  }

  @Override
  // subscriber from the requestChannel method
  public void subscribe(CoreSubscriber<? super Payload> actual) {

    long previousState = markSubscribed(STATE, this);
    if (isTerminated(previousState)) {
      Throwable t = Exceptions.terminate(INBOUND_ERROR, this);
      if (t != TERMINATED) {
        //noinspection ConstantConditions
        Operators.error(actual, t);
      } else {
        Operators.error(
            actual,
            new CancellationException("RequestChannelSubscriber has already been terminated"));
      }
      return;
    }

    if (isSubscribed(previousState)) {
      Operators.error(
          actual, new IllegalStateException("RequestChannelSubscriber allows only one Subscriber"));
      return;
    }

    this.inboundSubscriber = actual;
    // sends sender as a subscription since every request|cancel signal should be encoded to
    // requestNFrame|cancelFrame
    actual.onSubscribe(this);
  }

  @Override
  // subscription to the outbound
  public void onSubscribe(Subscription outboundSubscription) {
    if (Operators.validate(this.outboundSubscription, outboundSubscription)) {
      this.outboundSubscription = outboundSubscription;
      outboundSubscription.request(this.firstRequest);
    }
  }

  @Override
  public void request(long n) {
    if (!Operators.validate(n)) {
      return;
    }

    long previousState = StateUtils.addRequestN(STATE, this, n);
    if (isTerminated(previousState)) {
      // full termination can be the result of both sides completion / cancelFrame / remote or local
      // error
      // therefore, we need to check inbound error value, to see what should be done
      Throwable inboundError = Exceptions.terminate(INBOUND_ERROR, this);
      if (inboundError == TERMINATED) {
        // means inbound was already terminated
        return;
      }

      if (inboundError != null || this.inboundDone) {
        final CoreSubscriber<? super Payload> inboundSubscriber = this.inboundSubscriber;

        Payload firstPayload = this.firstPayload;
        if (firstPayload != null) {
          this.firstPayload = null;
          inboundSubscriber.onNext(firstPayload);
        }

        if (inboundError != null) {
          inboundSubscriber.onError(inboundError);
        } else {
          inboundSubscriber.onComplete();
        }
      }
      return;
    }

    if (isInboundTerminated(previousState)) {
      // inbound only can be terminated in case of cancellation or complete frame
      if (!hasRequested(previousState) && !isFirstFrameSent(previousState) && this.inboundDone) {
        final CoreSubscriber<? super Payload> inboundSubscriber = this.inboundSubscriber;

        final Payload firstPayload = this.firstPayload;
        this.firstPayload = null;

        inboundSubscriber.onNext(firstPayload);
        inboundSubscriber.onComplete();

        markFirstFrameSent(STATE, this);
      }
      return;
    }

    if (hasRequested(previousState)) {
      if (isFirstFrameSent(previousState)
          && !isMaxAllowedRequestN(StateUtils.extractRequestN(previousState))) {
        final int streamId = this.streamId;
        final ByteBuf requestNFrame = RequestNFrameCodec.encode(this.allocator, streamId, n);
        this.connection.sendFrame(streamId, requestNFrame);
      }
      return;
    }

    final CoreSubscriber<? super Payload> inboundSubscriber = this.inboundSubscriber;

    final Payload firstPayload = this.firstPayload;
    this.firstPayload = null;
    inboundSubscriber.onNext(firstPayload);

    previousState = markFirstFrameSent(STATE, this);
    if (isTerminated(previousState)) {
      // full termination can be the result of both sides completion / cancelFrame / remote or local
      // error
      // therefore, we need to check inbound error value, to see what should be done
      Throwable inboundError = Exceptions.terminate(INBOUND_ERROR, this);
      if (inboundError == TERMINATED) {
        // means inbound was already terminated
        return;
      }

      if (inboundError != null) {
        inboundSubscriber.onError(inboundError);
      } else if (this.inboundDone) {
        inboundSubscriber.onComplete();
      }
      return;
    }

    if (isInboundTerminated(previousState)) {
      // inbound only can be terminated in case of cancellation or complete frame
      if (this.inboundDone) {
        inboundSubscriber.onComplete();
      }
      return;
    }

    long requestN = StateUtils.extractRequestN(previousState);
    if (isMaxAllowedRequestN(requestN)) {
      final int streamId = this.streamId;
      final ByteBuf requestNFrame = RequestNFrameCodec.encode(allocator, streamId, requestN);
      this.connection.sendFrame(streamId, requestNFrame);
    } else {
      long firstRequestN = requestN - 1;
      if (firstRequestN > 0) {
        final int streamId = this.streamId;
        final ByteBuf requestNFrame =
            RequestNFrameCodec.encode(this.allocator, streamId, firstRequestN);
        this.connection.sendFrame(streamId, requestNFrame);
      }
    }
  }

  @Override
  // inbound cancellation
  public void cancel() {
    long previousState = markInboundTerminated(STATE, this);
    if (isTerminated(previousState) || isInboundTerminated(previousState)) {
      return;
    }

    if (!isFirstFrameSent(previousState) && !hasRequested(previousState)) {
      final Payload firstPayload = this.firstPayload;
      this.firstPayload = null;
      firstPayload.release();
    }

    final int streamId = this.streamId;

    if (isOutboundTerminated(previousState)) {
      this.requesterResponderSupport.remove(streamId, this);
    }

    final ByteBuf cancelFrame = CancelFrameCodec.encode(this.allocator, streamId);
    this.connection.sendFrame(streamId, cancelFrame);
  }

  @Override
  public final void handleCancel() {
    Subscription outboundSubscription = this.outboundSubscription;
    if (outboundSubscription == null) {
      // if subscription is null, it means that streams has not yet reassembled all the fragments
      // and fragmentation of the first frame was cancelled before
      lazyTerminate(STATE, this);

      this.requesterResponderSupport.remove(this.streamId, this);

      final CompositeByteBuf frames = this.frames;
      if (frames != null) {
        this.frames = null;
        frames.release();
      } else {
        final Payload firstPayload = this.firstPayload;
        this.firstPayload = null;
        firstPayload.release();
      }
      return;
    }

    this.tryTerminate(true);
  }

  final long tryTerminate(boolean isFromInbound) {
    Exceptions.addThrowable(
        INBOUND_ERROR, this, new CancellationException("Inbound has been canceled"));

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      return previousState;
    }

    this.requesterResponderSupport.remove(this.streamId, this);

    if (isReassembling(previousState)) {
      final CompositeByteBuf frames = this.frames;
      this.frames = null;
      if (isFromInbound) {
        frames.release();
      } else {
        synchronized (frames) {
          frames.release();
        }
      }
    }

    final Subscription outboundSubscription = this.outboundSubscription;
    if (outboundSubscription == null) {
      return previousState;
    }

    outboundSubscription.cancel();

    if (!isSubscribed(previousState)) {
      final Payload firstPayload = this.firstPayload;
      this.firstPayload = null;
      firstPayload.release();
    } else if (isFirstFrameSent(previousState) && !isInboundTerminated(previousState)) {
      Throwable inboundError = Exceptions.terminate(INBOUND_ERROR, this);
      if (inboundError != TERMINATED) {
        if (isFromInbound) {
          this.inboundDone = true;
          //noinspection ConstantConditions
          this.inboundSubscriber.onError(inboundError);
        } else {
          synchronized (this) {
            this.inboundDone = true;
            //noinspection ConstantConditions
            this.inboundSubscriber.onError(inboundError);
          }
        }
      }
    }

    return previousState;
  }

  final void handlePayload(Payload p) {
    synchronized (this) {
      if (this.inboundDone) {
        // payload from network so it has refCnt > 0
        p.release();
        return;
      }

      this.inboundSubscriber.onNext(p);
    }
  }

  @Override
  public final void handleError(Throwable t) {
    if (this.inboundDone) {
      Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
      return;
    }

    this.inboundDone = true;
    boolean wasThrowableAdded = Exceptions.addThrowable(INBOUND_ERROR, this, t);

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      if (!wasThrowableAdded) {
        Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
      }
      return;
    }

    this.requesterResponderSupport.remove(this.streamId, this);

    if (isReassembling(previousState)) {
      final CompositeByteBuf frames = this.frames;
      this.frames = null;
      frames.release();
    }

    if (!isSubscribed(previousState)) {
      final Payload firstPayload = this.firstPayload;
      this.firstPayload = null;
      firstPayload.release();
    } else if (isFirstFrameSent(previousState) && !isInboundTerminated(previousState)) {
      Throwable inboundError = Exceptions.terminate(INBOUND_ERROR, this);
      if (inboundError != TERMINATED) {
        //noinspection ConstantConditions
        this.inboundSubscriber.onError(inboundError);
      }
    }

    // this is downstream subscription so need to cancel it just in case error signal has not
    // reached it
    // needs for disconnected upstream and downstream case
    this.outboundSubscription.cancel();
  }

  @Override
  public void handleComplete() {
    if (this.inboundDone) {
      return;
    }

    this.inboundDone = true;

    long previousState = markInboundTerminated(STATE, this);

    if (isOutboundTerminated(previousState)) {
      this.requesterResponderSupport.remove(this.streamId, this);
    }

    if (isFirstFrameSent(previousState)) {
      this.inboundSubscriber.onComplete();
    }
  }

  @Override
  public void handleNext(ByteBuf frame, boolean hasFollows, boolean isLastPayload) {
    long state = this.state;
    if (isTerminated(state)) {
      return;
    }

    if (!hasFollows && !isReassembling(state)) {
      Payload payload;
      try {
        payload = this.payloadDecoder.apply(frame);
      } catch (Throwable t) {
        long previousState = this.tryTerminate(true);
        if (isTerminated(previousState) || isOutboundTerminated(previousState)) {
          Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
          return;
        }

        this.outboundDone = true;
        // send error to terminate interaction
        final int streamId = this.streamId;
        final ByteBuf errorFrame =
            ErrorFrameCodec.encode(this.allocator, streamId, new CanceledException(t.getMessage()));
        this.connection.sendFrame(streamId, errorFrame);

        return;
      }

      this.handlePayload(payload);
      if (isLastPayload) {
        this.handleComplete();
      }
      return;
    }

    CompositeByteBuf frames = this.frames;
    if (frames == null) {
      frames =
          ReassemblyUtils.addFollowingFrame(
              this.allocator.compositeBuffer(), frame, hasFollows, this.maxInboundPayloadSize);
      this.frames = frames;

      long previousState = markReassembling(STATE, this);
      if (isTerminated(previousState)) {
        this.frames = null;
        frames.release();
        return;
      }
    } else {
      try {
        frames =
            ReassemblyUtils.addFollowingFrame(
                frames, frame, hasFollows, this.maxInboundPayloadSize);
      } catch (IllegalStateException e) {
        if (isTerminated(this.state)) {
          return;
        }

        long previousState = this.tryTerminate(true);
        if (isTerminated(previousState) || isOutboundTerminated(previousState)) {
          Operators.onErrorDropped(e, this.inboundSubscriber.currentContext());
          return;
        }

        this.outboundDone = true;
        // send error to terminate interaction
        final int streamId = this.streamId;
        final ByteBuf errorFrame =
            ErrorFrameCodec.encode(
                this.allocator,
                streamId,
                new CanceledException("Failed to reassemble payload. Cause: " + e.getMessage()));
        this.connection.sendFrame(streamId, errorFrame);

        return;
      }
    }

    if (!hasFollows) {
      long previousState = markReassembled(STATE, this);
      if (isTerminated(previousState)) {
        return;
      }

      this.frames = null;

      Payload payload;
      try {
        payload = this.payloadDecoder.apply(frames);
        frames.release();
      } catch (Throwable t) {
        ReferenceCountUtil.safeRelease(frames);

        previousState = this.tryTerminate(true);
        if (isTerminated(previousState) || isOutboundTerminated(previousState)) {
          Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
          return;
        }

        // send error to terminate interaction
        final int streamId = this.streamId;
        final ByteBuf errorFrame =
            ErrorFrameCodec.encode(
                this.allocator,
                streamId,
                new CanceledException("Failed to reassemble payload. Cause: " + t.getMessage()));
        this.connection.sendFrame(streamId, errorFrame);

        return;
      }

      if (this.outboundSubscription == null) {
        this.firstPayload = payload;
        Flux<Payload> source = this.handler.requestChannel(this);
        source.subscribe(this);
      } else {
        this.handlePayload(payload);
      }

      if (isLastPayload) {
        this.handleComplete();
      }
    }
  }

  @Override
  public void onNext(Payload p) {
    if (this.outboundDone) {
      ReferenceCountUtil.safeRelease(p);
      return;
    }

    final int streamId = this.streamId;
    final DuplexConnection connection = this.connection;
    final ByteBufAllocator allocator = this.allocator;

    if (p == null) {
      final ByteBuf completeFrame = PayloadFrameCodec.encodeComplete(allocator, streamId);
      connection.sendFrame(streamId, completeFrame);
      return;
    }

    final int mtu = this.mtu;
    try {
      if (!isValid(mtu, this.maxFrameLength, p, false)) {
        p.release();

        // FIXME: must be scheduled on the connection event-loop to achieve serial
        //  behaviour on the inbound subscriber
        long previousState = this.tryTerminate(false);
        if (isTerminated(previousState) || isOutboundTerminated(previousState)) {
          Operators.onErrorDropped(
              new IllegalArgumentException(
                  String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength)),
              this.inboundSubscriber.currentContext());
          return;
        }

        final ByteBuf errorFrame =
            ErrorFrameCodec.encode(
                allocator,
                streamId,
                new CanceledException(
                    String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength)));
        connection.sendFrame(streamId, errorFrame);
        return;
      }
    } catch (IllegalReferenceCountException e) {

      // FIXME: must be scheduled on the connection event-loop to achieve serial
      //  behaviour on the inbound subscriber
      long previousState = this.tryTerminate(false);
      if (isTerminated(previousState) || isOutboundTerminated(previousState)) {
        Operators.onErrorDropped(e, this.inboundSubscriber.currentContext());
        return;
      }

      final ByteBuf errorFrame =
          ErrorFrameCodec.encode(
              allocator,
              streamId,
              new CanceledException("Failed to validate payload. Cause:" + e.getMessage()));
      connection.sendFrame(streamId, errorFrame);
      return;
    }

    try {
      sendReleasingPayload(streamId, FrameType.NEXT, mtu, p, connection, allocator, false);
    } catch (Throwable t) {
      // FIXME: must be scheduled on the connection event-loop to achieve serial
      //  behaviour on the inbound subscriber
      this.tryTerminate(false);
    }
  }

  @Override
  public void onError(Throwable t) {
    if (this.outboundDone) {
      Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
      return;
    }

    boolean wasThrowableAdded =
        Exceptions.addThrowable(
            INBOUND_ERROR,
            this,
            new CancellationException("Outbound has terminated with an error"));
    this.outboundDone = true;

    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      Operators.onErrorDropped(t, this.inboundSubscriber.currentContext());
      return;
    }

    final int streamId = this.streamId;

    this.requesterResponderSupport.remove(streamId, this);

    if (isReassembling(previousState)) {
      final CompositeByteBuf frames = this.frames;
      this.frames = null;
      synchronized (frames) {
        frames.release();
      }
    }

    if (!isFirstFrameSent(previousState)) {
      if (!hasRequested(previousState)) {
        final Payload firstPayload = this.firstPayload;
        this.firstPayload = null;
        firstPayload.release();
      }
    }

    if (wasThrowableAdded && !isInboundTerminated(previousState)) {
      Throwable inboundError = Exceptions.terminate(INBOUND_ERROR, this);
      if (inboundError != TERMINATED) {
        // FIXME: must be scheduled on the connection event-loop to achieve serial
        //  behaviour on the inbound subscriber
        synchronized (this) {
          this.inboundDone = true;
          //noinspection ConstantConditions
          this.inboundSubscriber.onError(inboundError);
        }
      }
    }

    final ByteBuf errorFrame = ErrorFrameCodec.encode(this.allocator, streamId, t);
    this.connection.sendFrame(streamId, errorFrame);
  }

  @Override
  public void onComplete() {
    if (this.outboundDone) {
      return;
    }

    this.outboundDone = true;

    long previousState = markOutboundTerminated(STATE, this, false);
    if (isTerminated(previousState)) {
      return;
    }

    final int streamId = this.streamId;

    if (isInboundTerminated(previousState)) {
      this.requesterResponderSupport.remove(streamId, this);
    }

    final ByteBuf completeFrame = PayloadFrameCodec.encodeComplete(this.allocator, streamId);
    this.connection.sendFrame(streamId, completeFrame);
  }

  @Override
  public final void handleRequestN(long n) {
    this.outboundSubscription.request(n);
  }

  @Override
  public Context currentContext() {
    return SendUtils.DISCARD_CONTEXT;
  }
}
