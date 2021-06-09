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
import io.aeron.logbuffer.BufferClaim;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Fuseable.QueueSubscription;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.util.context.Context;

class MonoSendMany implements Disposable, CoreSubscriber<ByteBuf>, Runnable {
  static final Logger logger = LoggerFactory.getLogger(MonoSendMany.class);

  static final ThreadLocal<BufferClaim> BUFFER_CLAIMS = ThreadLocal.withInitial(BufferClaim::new);
  static final ThreadLocal<UnsafeBuffer> UNSAFE_BUFFER = ThreadLocal.withInitial(UnsafeBuffer::new);

  final Sinks.Empty<Void> onClose;
  final EventLoop eventLoop;
  final ExclusivePublication publication;
  final int effort;
  final int prefetch;
  final int limit;

  volatile int wip;
  static final AtomicIntegerFieldUpdater<MonoSendMany> WIP =
      AtomicIntegerFieldUpdater.newUpdater(MonoSendMany.class, "wip");

  QueueSubscription<ByteBuf> subscription;

  ByteBuf undelivered;
  int produced;

  Throwable t;
  boolean done;
  boolean cancelled;

  public MonoSendMany(
      Sinks.Empty<Void> onClose,
      EventLoop eventLoop,
      ExclusivePublication publication,
      int effort,
      int prefetch) {
    this.onClose = onClose;
    this.eventLoop = eventLoop;
    this.publication = publication;
    this.effort = effort;
    this.prefetch = prefetch;
    this.limit = prefetch == Integer.MAX_VALUE ? Integer.MAX_VALUE : (prefetch - (prefetch >> 2));
  }

  @Override
  @SuppressWarnings("unchecked")
  public void onSubscribe(Subscription s) {
    if (Operators.validate(this.subscription, s)) {
      final QueueSubscription<ByteBuf> queueSubscription = (QueueSubscription<ByteBuf>) s;
      final int prefetch = this.prefetch;

      this.subscription = queueSubscription;

      // upstream always ASYNC fuseable
      queueSubscription.requestFusion(Fuseable.ANY);
      queueSubscription.request(prefetch == Integer.MAX_VALUE ? Long.MAX_VALUE : prefetch);
    }
  }

  @Override
  public void onNext(ByteBuf byteBuf) {
    drain();
  }

  @Override
  public void onError(Throwable t) {
    if (this.done) {
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    this.done = true;
    this.t = t;

    drain();
  }

  @Override
  public void onComplete() {
    if (this.done) {
      return;
    }

    this.done = true;

    drain();
  }

  @Override
  public void run() {
    drainAsync();
  }

  void drain() {
    if (WIP.getAndIncrement(this) != 0) {
      return;
    }

    drainAsync();
  }

  void drainAsync() {
    final Sinks.Empty<Void> onClose = this.onClose;
    final ExclusivePublication publication = this.publication;
    final QueueSubscription<ByteBuf> qs = this.subscription;
    final EventLoop eventLoop = this.eventLoop;
    final BufferClaim bufferClaim = BUFFER_CLAIMS.get();
    final UnsafeBuffer unsafeBuffer = UNSAFE_BUFFER.get();
    final int effort = this.effort;

    int missed = this.wip;
    int sent = this.produced;

    if (this.cancelled) {
      qs.cancel();
      publication.close();
      return;
    }

    final ByteBuf undelivered = this.undelivered;
    if (undelivered != null) {
      final boolean delivered;
      try {
        delivered =
            PublicationUtils.tryClaimOrOffer(
                undelivered, publication, bufferClaim, unsafeBuffer, effort);
      } catch (Throwable t) {
        undelivered.release();
        qs.cancel();

        this.done = true;
        onClose.tryEmitError(t);
        return;
      }

      if (!delivered) {
        eventLoop.schedule(this);
        return;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Emitted frame: \n{}\n", ByteBufUtil.prettyHexDump(undelivered));
      }

      this.undelivered = null;
      undelivered.release();

      sent++;

      if (sent == this.limit) {
        qs.request(this.limit);
        sent = 0;
      }
    }

    for (; ; ) {
      if (this.cancelled) {
        qs.cancel();
        publication.close();
        return;
      }

      for (; ; ) {
        final ByteBuf buf = qs.poll();

        final boolean empty = buf == null;
        if (empty) {
          if (this.done) {
            publication.close();

            final Throwable t = this.t;
            if (t == null) {
              onClose.tryEmitEmpty();
            } else {
              onClose.tryEmitError(t);
            }
          }
          break;
        }

        final boolean delivered;
        try {
          delivered =
              PublicationUtils.tryClaimOrOffer(buf, publication, bufferClaim, unsafeBuffer, effort);
        } catch (Throwable t) {
          qs.cancel();
          buf.release();

          this.done = true;
          onClose.tryEmitError(t);
          return;
        }

        if (!delivered) {
          this.undelivered = buf;
          this.produced = sent;
          eventLoop.schedule(this);
          return;
        }

        if (logger.isDebugEnabled()) {
          logger.debug("Emitted frame: \n{}\n", ByteBufUtil.prettyHexDump(buf));
        }

        buf.release();

        sent++;

        if (sent == this.limit) {
          qs.request(this.limit);
          sent = 0;
        }
      }

      int w = this.wip;
      if (missed == w) {
        this.produced = sent;
        missed = WIP.addAndGet(this, -missed);
        if (missed == 0) {
          break;
        }
      } else {
        missed = w;
      }
    }
  }

  @Override
  public void dispose() {
    if (this.cancelled) {
      return;
    }

    this.cancelled = true;

    if (WIP.getAndIncrement(this) == 0) {
      this.subscription.cancel();
    }
  }
}
