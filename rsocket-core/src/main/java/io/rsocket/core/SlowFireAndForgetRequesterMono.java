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
import static io.rsocket.core.StateUtils.isPrepared;
import static io.rsocket.core.StateUtils.isSubscribedOrTerminated;
import static io.rsocket.core.StateUtils.isTerminated;
import static io.rsocket.core.StateUtils.lazyTerminate;
import static io.rsocket.core.StateUtils.markPrepared;
import static io.rsocket.core.StateUtils.markSubscribed;
import static io.rsocket.core.StateUtils.markTerminated;

import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.frame.FrameType;
import io.rsocket.plugins.RequestInterceptor;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

final class SlowFireAndForgetRequesterMono extends Mono<Void>
    implements LeaseHandler, Subscription, Scannable {

  volatile long state;

  static final AtomicLongFieldUpdater<SlowFireAndForgetRequesterMono> STATE =
      AtomicLongFieldUpdater.newUpdater(SlowFireAndForgetRequesterMono.class, "state");

  final Payload payload;

  final ByteBufAllocator allocator;
  final int mtu;
  final int maxFrameLength;
  final RequesterResponderSupport requesterResponderSupport;
  final DuplexConnection connection;

  @Nullable final RequesterLeaseTracker leaseTracker;
  @Nullable final RequestInterceptor requestInterceptor;

  CoreSubscriber<? super Void> actual;

  SlowFireAndForgetRequesterMono(
      Payload payload, RequesterResponderSupport requesterResponderSupport) {
    this.allocator = requesterResponderSupport.getAllocator();
    this.payload = payload;
    this.mtu = requesterResponderSupport.getMtu();
    this.maxFrameLength = requesterResponderSupport.getMaxFrameLength();
    this.requesterResponderSupport = requesterResponderSupport;
    this.connection = requesterResponderSupport.getDuplexConnection();
    this.requestInterceptor = requesterResponderSupport.getRequestInterceptor();
    this.leaseTracker = requesterResponderSupport.getRequesterLeaseTracker();
  }

  @Override
  public void subscribe(CoreSubscriber<? super Void> actual) {
    final RequesterLeaseTracker leaseHandler = this.leaseTracker;
    final boolean leaseEnabled = leaseHandler != null;
    long previousState = markSubscribed(STATE, this, !leaseEnabled);
    if (isSubscribedOrTerminated(previousState)) {
      final IllegalStateException e =
          new IllegalStateException("FireAndForgetMono allows only a single Subscriber");

      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onReject(e, FrameType.REQUEST_FNF, null);
      }

      Operators.error(actual, e);
      return;
    }

    final Payload p = this.payload;
    int mtu = this.mtu;
    try {
      if (!isValid(mtu, this.maxFrameLength, p, false)) {
        lazyTerminate(STATE, this);

        final IllegalArgumentException e =
            new IllegalArgumentException(
                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength));
        final RequestInterceptor requestInterceptor = this.requestInterceptor;
        if (requestInterceptor != null) {
          requestInterceptor.onReject(e, FrameType.REQUEST_FNF, p.metadata());
        }

        p.release();

        Operators.error(actual, e);
        return;
      }
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);

      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onReject(e, FrameType.REQUEST_FNF, null);
      }

      Operators.error(actual, e);
      return;
    }

    this.actual = actual;
    actual.onSubscribe(this);

    if (leaseEnabled) {
      leaseHandler.issue(this);
      return;
    }

    sendFirstFrame(p);
  }

  @Override
  public void handleLease() {
    final long previousState = markPrepared(STATE, this);

    if (isTerminated(previousState)) {
      return;
    }

    sendFirstFrame(this.payload);
  }

  void sendFirstFrame(Payload p) {
    final CoreSubscriber<? super Void> actual = this.actual;
    final int streamId;
    try {
      streamId = this.requesterResponderSupport.getNextStreamId();
    } catch (Throwable t) {
      lazyTerminate(STATE, this);

      final Throwable ut = Exceptions.unwrap(t);
      final RequestInterceptor requestInterceptor = this.requestInterceptor;
      if (requestInterceptor != null) {
        requestInterceptor.onReject(ut, FrameType.REQUEST_FNF, p.metadata());
      }

      p.release();

      actual.onError(ut);
      return;
    }

    final RequestInterceptor interceptor = this.requestInterceptor;
    if (interceptor != null) {
      interceptor.onStart(streamId, FrameType.REQUEST_FNF, p.metadata());
    }

    try {
      if (isTerminated(this.state)) {
        p.release();

        if (interceptor != null) {
          interceptor.onCancel(streamId, FrameType.REQUEST_FNF);
        }

        return;
      }

      sendReleasingPayload(
          streamId, FrameType.REQUEST_FNF, mtu, p, this.connection, this.allocator, true);
    } catch (Throwable e) {
      lazyTerminate(STATE, this);

      if (interceptor != null) {
        interceptor.onTerminate(streamId, FrameType.REQUEST_FNF, e);
      }

      actual.onError(e);
      return;
    }

    lazyTerminate(STATE, this);

    if (interceptor != null) {
      interceptor.onTerminate(streamId, FrameType.REQUEST_FNF, null);
    }

    actual.onComplete();
  }

  @Override
  public void request(long n) {
    // no ops
  }

  @Override
  public void cancel() {
    final long previousState = markTerminated(STATE, this);

    if (isTerminated(previousState)) {
      return;
    }

    if (!isPrepared(previousState)) {
      this.payload.release();
    }
  }

  @Override
  public final void handleError(Throwable cause) {
    long previousState = markTerminated(STATE, this);
    if (isTerminated(previousState)) {
      Operators.onErrorDropped(cause, this.actual.currentContext());
      return;
    }

    final Payload p = this.payload;
    final RequestInterceptor requestInterceptor = this.requestInterceptor;
    if (requestInterceptor != null) {
      requestInterceptor.onReject(cause, FrameType.REQUEST_RESPONSE, p.metadata());
    }

    p.release();

    this.actual.onError(cause);
  }

  @Override
  public Object scanUnsafe(Attr key) {
    return null; // no particular key to be represented, still useful in hooks
  }

  @Override
  @NonNull
  public String stepName() {
    return "source(FireAndForgetMono)";
  }
}
