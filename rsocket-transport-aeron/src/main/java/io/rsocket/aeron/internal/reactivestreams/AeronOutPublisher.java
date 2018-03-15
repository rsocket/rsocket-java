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

import io.aeron.ControlledFragmentAssembler;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.rsocket.aeron.internal.EventLoop;
import io.rsocket.aeron.internal.NotConnectedException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.IntSupplier;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

/** */
public class AeronOutPublisher extends Flux<DirectBuffer> {
  private static final Logger logger = LoggerFactory.getLogger(AeronOutPublisher.class);
  private final io.aeron.Subscription source;
  private final EventLoop eventLoop;

  private String name;
  private volatile long requested;
  private volatile long processed;
  private Subscriber<? super DirectBuffer> destination;
  private AeronOutProcessorSubscription subscription;
  private final int sessionId;

  /**
   * Creates a publication for a unique session
   *
   * @param name publication's name
   * @param sessionId sessionId between the source and the remote publication
   * @param source Aeron {@code Subscription} publish data from
   * @param eventLoop {@link EventLoop} to poll the source with
   */
  public AeronOutPublisher(
      String name, int sessionId, io.aeron.Subscription source, EventLoop eventLoop) {
    this.name = name;
    this.source = source;
    this.eventLoop = eventLoop;
    this.sessionId = sessionId;
  }

  @Override
  public void subscribe(CoreSubscriber<? super DirectBuffer> destination) {
    Objects.requireNonNull(destination);
    synchronized (this) {
      if (this.destination != null && subscription.canEmit()) {
        throw new IllegalStateException(
            "only allows one subscription => channel "
                + source.channel()
                + " and stream id => "
                + source.streamId());
      }
      this.destination = destination;
    }

    this.subscription = new AeronOutProcessorSubscription(destination);
    destination.onSubscribe(subscription);
  }

  void onError(Throwable t) {
    subscription.erred = true;
    if (destination != null) {
      destination.onError(t);
    }
  }

  void cancel() {
    if (subscription != null) {
      subscription.cancel();
    }
  }

  @Override
  public String toString() {
    return "AeronOutPublisher{" + "name='" + name + '\'' + '}';
  }

  private class AeronOutProcessorSubscription implements Subscription {
    private volatile boolean erred = false;
    private volatile boolean cancelled = false;
    private final Subscriber<? super DirectBuffer> destination;
    private final ControlledFragmentAssembler assembler;

    public AeronOutProcessorSubscription(Subscriber<? super DirectBuffer> destination) {
      this.destination = destination;
      this.assembler = new ControlledFragmentAssembler(this::onFragment, 4096);
    }

    boolean emitting = false;
    boolean missed = false;

    @Override
    public void request(long n) {
      if (n < 0) {
        onError(new IllegalStateException("n must be greater than zero"));
      }

      synchronized (AeronOutPublisher.this) {
        long r;
        if (requested != Long.MAX_VALUE && n > 0) {
          r = requested + n;
          requested = r < 0 ? Long.MAX_VALUE : r;
        }
      }

      tryEmit();
    }

    // allocate this once
    final IntSupplier supplier = this::emit;

    void tryEmit() {
      synchronized (AeronOutPublisher.this) {
        if (emitting) {
          missed = true;
          return;
        }
        emitting = true;
        eventLoop.execute(supplier);
      }
    }

    ControlledFragmentHandler.Action onFragment(
        DirectBuffer buffer, int offset, int length, Header header) {
      if (sessionId != header.sessionId()) {
        if (source.imageBySessionId(header.sessionId()) == null) {
          return ControlledFragmentHandler.Action.CONTINUE;
        }

        return ControlledFragmentHandler.Action.ABORT;
      }

      try {
        ByteBuffer bytes = ByteBuffer.allocate(length);
        buffer.getBytes(offset, bytes, length);

        if (canEmit()) {
          destination.onNext(new UnsafeBuffer(bytes));
        }
      } catch (Throwable t) {
        onError(t);
      }

      return ControlledFragmentHandler.Action.COMMIT;
    }

    int emit() {
      int emitted = 0;
      for (; ; ) {
        synchronized (AeronOutPublisher.this) {
          missed = false;
        }

        try {
          if (source.isClosed()) {
            onError(new NotConnectedException(name));
            return 0;
          }

          while (processed < requested) {

            int poll = source.controlledPoll(assembler, 4096);

            if (poll < 1) {
              break;
            } else {
              emitted++;
              processed++;
            }
          }

          synchronized (AeronOutPublisher.this) {
            emitting = false;
            break;
          }

        } catch (Throwable t) {
          onError(t);
        }
      }

      if (canEmit()) {
        tryEmit();
      }

      return emitted;
    }

    @Override
    public void cancel() {
      cancelled = true;
    }

    private boolean canEmit() {
      return !cancelled && !erred;
    }
  }
}
