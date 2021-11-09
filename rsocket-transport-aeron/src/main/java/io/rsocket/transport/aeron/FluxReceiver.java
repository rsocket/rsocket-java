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

import static io.rsocket.transport.aeron.Operators.addCapCancellable;
import static io.rsocket.transport.aeron.Operators.removeCapCancellable;

import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;

class FluxReceiver extends Flux<ByteBuf>
    implements org.reactivestreams.Subscription, ControlledFragmentHandler, Runnable {

  static final Logger logger = LoggerFactory.getLogger(FluxReceiver.class);
  static final ThreadLocal<WrappedDirectBufferByteBuf> WRAPPED_DIRECT_BUFFER_BYTE_BUF =
      ThreadLocal.withInitial(WrappedDirectBufferByteBuf::new);

  final Subscription subscription;
  final ControlledFragmentAssembler assembler;
  final EventLoop eventLoop;
  final Sinks.Empty<Void> onClose;
  final int effort;

  volatile long requested;
  static final AtomicLongFieldUpdater<FluxReceiver> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(FluxReceiver.class, "requested");

  static final long INITIAL = -1;
  static final long SUBSCRIBED = 0;
  static final long DISPOSED = Long.MIN_VALUE + 1;
  static final long CANCELLED = Long.MIN_VALUE;

  CoreSubscriber<? super ByteBuf> actual;

  int produced;

  public FluxReceiver(
      Sinks.Empty<Void> onClose, EventLoop eventLoop, Subscription subscription, int effort) {
    this.onClose = onClose;
    this.eventLoop = eventLoop;
    this.subscription = subscription;
    this.assembler = new ControlledFragmentAssembler(this);
    this.effort = effort;
    REQUESTED.lazySet(this, INITIAL);
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
    if (this.requested == INITIAL && REQUESTED.compareAndSet(this, INITIAL, SUBSCRIBED)) {
      this.actual = actual;
      actual.onSubscribe(this);
    } else {
      Operators.error(
          actual, new IllegalStateException("FluxReceiver allows only a single Subscriber"));
    }
  }

  @Override
  public void request(long n) {
    if (Operators.validate(n)) {
      if (addCapCancellable(REQUESTED, this, n) == 0) {
        drain(n);
      }
    }
  }

  @Override
  public void run() {
    final long requested = this.requested;

    if (requested == CANCELLED) {
      if (logger.isDebugEnabled()) {
        logger.debug("Closing Aeron Subscription due to cancellation");
      }
      this.subscription.close();
      return;
    } else if (requested == DISPOSED) {
      if (logger.isDebugEnabled()) {
        logger.debug("Closing Aeron Subscription due to disposure");
      }
      this.actual.onError(new CancellationException("Disposed"));
      this.subscription.close();
      return;
    }

    drain(requested);
  }

  void drain(long n) {
    final Subscription subscription = this.subscription;
    final ControlledFragmentAssembler assembler = this.assembler;

    int produced;
    int effort = this.effort;

    for (; ; ) {
      boolean consumed = false;

      for (; ; ) {
        int polled = subscription.controlledPoll(assembler, effort);

        produced = this.produced;
        if (produced == -1) {
          subscription.close();
          return;
        } else if (produced == -2) {
          this.actual.onError(new CancellationException("Disposed"));
          subscription.close();
          return;
        }

        if (polled < 1) {
          if (subscription.isClosed()) {
            final NotConnectedException exception =
                new NotConnectedException("Aeron Subscription has been closed unexpectedly");
            this.actual.onError(exception);
            this.onClose.tryEmitError(exception);
            return;
          }

          --effort;
        } else {
          effort -= polled;

          if (produced >= n) {
            consumed = true;
            break;
          }
        }

        if (effort == 0) {
          break;
        }
      }

      if (consumed) {
        n = removeCapCancellable(REQUESTED, this, produced);
        if (n == CANCELLED) {
          if (logger.isDebugEnabled()) {
            logger.debug("Closing Aeron Subscription due to cancellation");
          }
          subscription.close();
          return;
        } else if (n == DISPOSED) {
          if (logger.isDebugEnabled()) {
            logger.debug("Closing Aeron Subscription due to disposure");
          }
          this.actual.onError(new CancellationException("Disposed"));
          subscription.close();
          return;
        } else if (n == 0) {
          this.produced = 0;
          return;
        }
      } else {
        this.eventLoop.schedule(this);
        return;
      }
    }
  }

  @Override
  public Action onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    final long requested = this.requested;
    if (requested == CANCELLED) {
      this.produced = -1;
      return Action.ABORT;
    }

    if (requested == DISPOSED) {
      this.produced = -2;
      return Action.ABORT;
    }

    final WrappedDirectBufferByteBuf wrappedDirectBuffer = WRAPPED_DIRECT_BUFFER_BYTE_BUF.get();
    wrappedDirectBuffer.wrap(buffer, offset, offset + length);

    if (logger.isDebugEnabled()) {
      logger.debug("Receiving:\n{}\n", ByteBufUtil.prettyHexDump(wrappedDirectBuffer));
    }

    this.actual.onNext(wrappedDirectBuffer);

    if (requested != Long.MAX_VALUE) {
      this.produced++;

      if (this.produced == requested) {
        return Action.BREAK;
      }
    }

    return Action.CONTINUE;
  }

  @Override
  public void cancel() {
    long state = this.requested;
    if (state == CANCELLED || state == DISPOSED) {
      return;
    }

    state = REQUESTED.getAndSet(this, CANCELLED);
    if (state == CANCELLED || state == DISPOSED) {
      return;
    }

    if (state == 0) {
      this.subscription.close();
    }
  }

  public void dispose() {
    long state = this.requested;
    if (state == CANCELLED || state == DISPOSED) {
      return;
    }

    state = REQUESTED.getAndSet(this, DISPOSED);
    if (state == CANCELLED || state == DISPOSED) {
      return;
    }

    if (state == 0) {
      this.subscription.close();
    }
  }
}
