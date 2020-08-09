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

import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.frame.FrameType;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

final class FireAndForgetRequesterMono extends Mono<Void> implements Subscription, Scannable {

  volatile long state;

  static final AtomicLongFieldUpdater<FireAndForgetRequesterMono> STATE =
      AtomicLongFieldUpdater.newUpdater(FireAndForgetRequesterMono.class, "state");

  final Payload payload;

  final ByteBufAllocator allocator;
  final int mtu;
  final int maxFrameLength;
  final RequesterResponderSupport requesterResponderSupport;
  final DuplexConnection connection;

  FireAndForgetRequesterMono(Payload payload, RequesterResponderSupport requesterResponderSupport) {
    this.allocator = requesterResponderSupport.getAllocator();
    this.payload = payload;
    this.mtu = requesterResponderSupport.getMtu();
    this.maxFrameLength = requesterResponderSupport.getMaxFrameLength();
    this.requesterResponderSupport = requesterResponderSupport;
    this.connection = requesterResponderSupport.getDuplexConnection();
  }

  @Override
  public void subscribe(CoreSubscriber<? super Void> actual) {
    long previousState = markSubscribed(STATE, this);
    if (isSubscribedOrTerminated(previousState)) {
      Operators.error(
          actual, new IllegalStateException("FireAndForgetMono allows only a single Subscriber"));
      return;
    }

    actual.onSubscribe(this);

    final Payload p = this.payload;
    int mtu = this.mtu;
    try {
      if (!isValid(mtu, this.maxFrameLength, p, false)) {
        lazyTerminate(STATE, this);
        p.release();
        actual.onError(
            new IllegalArgumentException(
                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength)));
        return;
      }
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);
      actual.onError(e);
      return;
    }

    final int streamId;
    try {
      streamId = this.requesterResponderSupport.getNextStreamId();
    } catch (Throwable t) {
      lazyTerminate(STATE, this);
      p.release();
      actual.onError(Exceptions.unwrap(t));
      return;
    }

    try {
      if (isTerminated(this.state)) {
        p.release();
        return;
      }

      sendReleasingPayload(
          streamId, FrameType.REQUEST_FNF, mtu, p, this.connection, this.allocator, true);
    } catch (Throwable e) {
      lazyTerminate(STATE, this);
      actual.onError(e);
      return;
    }

    lazyTerminate(STATE, this);
    actual.onComplete();
  }

  @Override
  public void request(long n) {
    // no ops
  }

  @Override
  public void cancel() {
    markTerminated(STATE, this);
  }

  @Override
  @Nullable
  public Void block(Duration m) {
    return block();
  }

  @Override
  @Nullable
  public Void block() {
    long previousState = markSubscribed(STATE, this);
    if (isSubscribedOrTerminated(previousState)) {
      throw new IllegalStateException("FireAndForgetMono allows only a single Subscriber");
    }

    final Payload p = this.payload;
    try {
      if (!isValid(this.mtu, this.maxFrameLength, p, false)) {
        lazyTerminate(STATE, this);
        p.release();
        throw new IllegalArgumentException(
            String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength));
      }
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);
      throw Exceptions.propagate(e);
    }

    final int streamId;
    try {
      streamId = this.requesterResponderSupport.getNextStreamId();
    } catch (Throwable t) {
      lazyTerminate(STATE, this);
      p.release();
      throw Exceptions.propagate(t);
    }

    try {
      sendReleasingPayload(
          streamId,
          FrameType.REQUEST_FNF,
          this.mtu,
          this.payload,
          this.connection,
          this.allocator,
          true);
    } catch (Throwable e) {
      lazyTerminate(STATE, this);
      throw Exceptions.propagate(e);
    }

    lazyTerminate(STATE, this);
    return null;
  }

  @Override
  public Object scanUnsafe(Scannable.Attr key) {
    return null; // no particular key to be represented, still useful in hooks
  }

  @Override
  @NonNull
  public String stepName() {
    return "source(FireAndForgetMono)";
  }
}
