/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.aeron.internal.reactivestreams;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import io.rsocket.aeron.internal.Constants;
import io.rsocket.aeron.internal.NotConnectedException;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class AeronInSubscriber implements Subscriber<DirectBuffer> {
  private static final Logger logger = LoggerFactory.getLogger(AeronInSubscriber.class);
  private static final ThreadLocal<BufferClaim> bufferClaims =
      ThreadLocal.withInitial(BufferClaim::new);
  private static final int BUFFER_SIZE = 128;
  private static final int REFILL = BUFFER_SIZE / 3;

  private static final OneToOneConcurrentArrayQueue<OneToOneConcurrentArrayQueue<DirectBuffer>>
      queues = new OneToOneConcurrentArrayQueue<>(BUFFER_SIZE);

  private final OneToOneConcurrentArrayQueue<DirectBuffer> buffers;
  private final String name;
  private final Publication destination;

  private Subscription subscription;

  private volatile boolean complete;
  private volatile boolean erred = false;

  private volatile long requested;

  public AeronInSubscriber(String name, Publication destination) {
    this.name = name;
    this.destination = destination;
    OneToOneConcurrentArrayQueue<DirectBuffer> poll;
    synchronized (queues) {
      poll = queues.poll();
    }
    buffers = poll != null ? poll : new OneToOneConcurrentArrayQueue<>(BUFFER_SIZE);
  }

  @Override
  public synchronized void onSubscribe(Subscription subscription) {
    this.subscription = subscription;
    requested = BUFFER_SIZE;
    subscription.request(BUFFER_SIZE);
  }

  @Override
  public void onNext(DirectBuffer buffer) {
    if (!erred) {
      if (logger.isTraceEnabled()) {
        logger.trace(
            name
                + " sending to destination => "
                + destination.channel()
                + " and aeron stream "
                + destination.streamId()
                + " and session id "
                + destination.sessionId());
      }
      boolean offer;
      synchronized (buffers) {
        offer = buffers.offer(buffer);
      }
      if (!offer) {
        onError(new IllegalStateException("missing back-pressure"));
      }

      tryEmit();
    }
  }

  private boolean emitting = false;
  private boolean missed = false;

  void tryEmit() {
    synchronized (this) {
      if (emitting) {
        missed = true;
        return;
      }
    }

    emit();
  }

  void emit() {
    try {
      for (; ; ) {
        synchronized (this) {
          missed = false;
        }
        while (!buffers.isEmpty()) {
          DirectBuffer buffer = buffers.poll();
          tryClaimOrOffer(buffer);
          requested--;
          if (requested < REFILL) {
            synchronized (buffers) {
              if (!complete) {
                long diff = BUFFER_SIZE - requested;
                requested = BUFFER_SIZE;
                subscription.request(diff);
              }
            }
          }
        }

        synchronized (this) {
          if (!missed) {
            emitting = false;
            break;
          }
        }
      }
    } catch (Throwable t) {
      onError(t);
    }

    if (complete && buffers.isEmpty()) {
      synchronized (queues) {
        queues.offer(buffers);
      }
    }
  }

  private void tryClaimOrOffer(DirectBuffer buffer) {
    boolean successful = false;

    int capacity = buffer.capacity();
    if (capacity < Constants.AERON_MTU_SIZE) {
      BufferClaim bufferClaim = bufferClaims.get();

      while (!successful) {
        long offer = destination.tryClaim(capacity, bufferClaim);
        if (offer >= 0) {
          try {
            final MutableDirectBuffer b = bufferClaim.buffer();
            int offset = bufferClaim.offset();
            b.putBytes(offset, buffer, 0, capacity);
          } finally {
            bufferClaim.commit();
            successful = true;
          }
        } else {
          if (offer == Publication.CLOSED) {
            onError(new NotConnectedException(name));
          }

          successful = false;
        }
      }

    } else {
      while (!successful) {
        long offer = destination.offer(buffer);

        if (offer < 0) {
          if (offer == Publication.CLOSED) {
            onError(new NotConnectedException(name));
          }
        } else {
          successful = true;
        }
      }
    }
  }

  @Override
  public synchronized void onError(Throwable t) {
    if (!erred) {
      erred = true;
      subscription.cancel();
    }

    t.printStackTrace();
  }

  @Override
  public synchronized void onComplete() {
    complete = true;
    tryEmit();
  }

  @Override
  public String toString() {
    return "AeronInSubscriber{" + "name='" + name + '\'' + '}';
  }
}
