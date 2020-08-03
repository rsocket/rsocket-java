package io.rsocket.transport.shm;

import static io.rsocket.transport.shm.OperatorsSupport.addCapCancellable;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.transport.shm.buffer.ReaderBuffer;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;

final class FluxReceive extends Flux<ByteBuf> implements Subscription, Runnable {

  final ReaderBuffer reader;
  final ByteBufAllocator alloc;
  final Scheduler.Worker eventLoop;

  volatile long requested;
  static final AtomicLongFieldUpdater<FluxReceive> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(FluxReceive.class, "requested");

  CoreSubscriber<? super ByteBuf> actual;

  long produced;

  FluxReceive(ReaderBuffer reader, ByteBufAllocator alloc, Scheduler.Worker eventLoop) {
    this.alloc = alloc;
    this.eventLoop = eventLoop;
    this.reader = reader;

    REQUESTED.lazySet(this, -1);
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
    if (requested == -1 && REQUESTED.compareAndSet(this, -1, 0)) {
      this.actual = actual;
      actual.onSubscribe(this);
    } else {
      Operators.error(actual, new IllegalStateException("Only a single subscriber is allowed"));
    }
  }

  @Override
  public void request(long n) {
    long state = addCapCancellable(REQUESTED, this, n);

    if (state == Long.MIN_VALUE || state == Long.MAX_VALUE) {
      return;
    }

    if (state == 0) {
      eventLoop.schedule(this);
    }
  }

  @Override
  public void cancel() {
    long state = REQUESTED.getAndSet(this, Long.MIN_VALUE);

    if (state == Long.MIN_VALUE) {
      return;
    }

    eventLoop.dispose();
  }

  void doComplete(Subscriber<?> a) {
    a.onComplete();
    eventLoop.dispose();
  }

  void doError(Subscriber<?> a, Throwable e) {
    try {
      a.onError(e);
    } finally {
      eventLoop.dispose();
    }
  }

  @Override
  public void run() {
    final ReaderBuffer reader = this.reader;
    final CoreSubscriber<? super ByteBuf> a = this.actual;

    long e = produced;
    long r = requested;

    for (; ; ) {

      if (r == Long.MIN_VALUE) {
        return;
      }

      while (r != e) {
        boolean d = reader.isClosed();

        ByteBuf byteBuffer;
        try {
          byteBuffer = reader.read();
        } catch (Throwable ex) {
          doError(a, ex);
          return;
        }

        boolean empty = byteBuffer == null;

        if (empty) {
          if (requested == Long.MIN_VALUE) {
            return;
          }
          if (d) {
            a.onComplete();
            return;
          }
          produced = e;
          eventLoop.schedule(this);
          return;
        }

        a.onNext(byteBuffer);
        reader.advance(1);

        e++;
      }

      if (reader.isClosed()) {
        a.onComplete();
        return;
      }

      if (e != 0 && r != Long.MAX_VALUE) {
        produced = 0;
        e = 0;
        r = addCapCancellable(REQUESTED, this, -e);

        if (r == Long.MIN_VALUE) {
          return;
        }

        if (r == 0) {
          return;
        }
      } else {
        r = requested;
      }
    }
  }
}
