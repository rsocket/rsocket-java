package io.rsocket.transport.shm;

import static io.rsocket.transport.shm.OperatorsSupport.discardAsyncWithTermination;
import static io.rsocket.transport.shm.OperatorsSupport.discardWithTermination;
import static reactor.core.Fuseable.ASYNC;
import static reactor.core.Fuseable.SYNC;

import io.netty.buffer.ByteBuf;
import io.rsocket.transport.shm.buffer.WriterBuffer;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

final class MonoSendMany extends Mono<Void>
    implements CoreSubscriber<ByteBuf>, Subscription, Runnable {

  static final Consumer<ByteBuf> RELEASER =
      (bb) -> {
        try {
          bb.release();
        } catch (Throwable e) {
          // ignored
        }
      };
  static final Context ON_DISCARD_SUPPORT_CONTEXT = Operators.enableOnDiscard(null, RELEASER);

  final WriterBuffer writer;

  final Publisher<ByteBuf> source;
  final Scheduler.Worker eventLoop;
  final Supplier<Queue<ByteBuf>> queueSupplier;

  final int prefetch;
  final int limit;

  static final int INITIAL_STATE = -2;
  static final int SUBSCRIBED_STATE = -1;
  static final int CANCELLED_STATE = 1;
  static final int NO_WORK_IN_PROGRESS_STATE = 0;
  static final int TERMINATED_WORK_IN_PROGRESS_STATE = Integer.MIN_VALUE;

  volatile int wip;
  static final AtomicIntegerFieldUpdater<MonoSendMany> WIP =
      AtomicIntegerFieldUpdater.newUpdater(MonoSendMany.class, "wip");

  volatile int canceled;
  static final AtomicIntegerFieldUpdater<MonoSendMany> CANCELED =
      AtomicIntegerFieldUpdater.newUpdater(MonoSendMany.class, "canceled");

  CoreSubscriber<? super Void> actual;
  Subscription s;
  int sourceMode;
  Queue<ByteBuf> queue;

  int produced;

  ByteBuf remainingBuffer;

  boolean done;
  Throwable error;

  MonoSendMany(
      WriterBuffer writer, Publisher<ByteBuf> source, Scheduler.Worker eventLoop, int prefetch) {

    this.writer = writer;
    this.source = source;
    this.eventLoop = eventLoop;
    this.prefetch = prefetch;
    this.queueSupplier = Queues.get(prefetch);
    this.limit = prefetch == Integer.MAX_VALUE ? Integer.MAX_VALUE : (prefetch - (prefetch >> 2));

    WIP.lazySet(this, INITIAL_STATE);
  }

  @Override
  public void subscribe(CoreSubscriber<? super Void> actual) {
    if (wip == INITIAL_STATE && WIP.compareAndSet(this, INITIAL_STATE, SUBSCRIBED_STATE)) {
      this.actual = actual;
      this.source.subscribe(this);
    } else {
      Operators.error(actual, new IllegalStateException("Only a single subscriber is allowed"));
    }
  }

  @Override
  public Context currentContext() {
    return ON_DISCARD_SUPPORT_CONTEXT;
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (Operators.validate(this.s, s)) {
      this.s = s;
      if (s instanceof Fuseable.QueueSubscription) {
        @SuppressWarnings("unchecked")
        Fuseable.QueueSubscription<ByteBuf> f = (Fuseable.QueueSubscription<ByteBuf>) s;

        int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);

        if (m == Fuseable.SYNC) {
          sourceMode = Fuseable.SYNC;
          queue = f;
          done = true;

          actual.onSubscribe(this);
          return;
        }
        if (m == ASYNC) {
          sourceMode = ASYNC;
          queue = f;

          actual.onSubscribe(this);
          return;
        }
      }

      queue = queueSupplier.get();

      actual.onSubscribe(this);
    }
  }

  @Override
  public void onNext(ByteBuf buf) {
    if (sourceMode == ASYNC) {
      trySchedule(null);
      return;
    }

    if (done) {
      buf.release();
      return;
    }

    if (!queue.offer(buf)) {
      buf.release();
      error =
          Operators.onOperatorError(
              s,
              Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
              buf,
              actual.currentContext());
      done = true;
    }

    trySchedule(buf);
  }

  @Override
  public void onError(Throwable t) {
    if (done) {
      return;
    }

    error = t;
    done = true;

    trySchedule(null);
  }

  @Override
  public void onComplete() {
    if (done) {
      return;
    }

    done = true;

    trySchedule(null);
  }

  @Override
  public void request(long n) {
    if (wip == SUBSCRIBED_STATE
        & WIP.compareAndSet(this, SUBSCRIBED_STATE, NO_WORK_IN_PROGRESS_STATE)) {
      if (sourceMode != Fuseable.SYNC) {
        this.s.request(prefetch);
      }

      trySchedule(null);
    }
  }

  @Override
  public void cancel() {
    int state = CANCELED.getAndSet(this, CANCELLED_STATE);

    if (state == CANCELLED_STATE) {
      return;
    }

    eventLoop.dispose();

    int wip = OperatorsSupport.wipIncrement(WIP, this);
    if (wip == NO_WORK_IN_PROGRESS_STATE) {
      discardWithTermination(WIP, this, queue, currentContext());
    }
  }

  void trySchedule(@Nullable ByteBuf buf) {
    int wip = OperatorsSupport.wipIncrement(WIP, this);
    if (wip == TERMINATED_WORK_IN_PROGRESS_STATE) {
      if (buf != null) {
        RELEASER.accept(buf);
      } else if (sourceMode == ASYNC) {
        // delegates discarding to the queue holder to ensure there is no racing on draining from
        // the SpScQueue
        queue.clear();
      }
      return;
    } else if (wip > NO_WORK_IN_PROGRESS_STATE) {
      return;
    }

    try {
      eventLoop.schedule(this);
    } catch (RejectedExecutionException ree) {
      Queue<ByteBuf> q = this.queue;
      if (sourceMode == ASYNC) {
        discardAsyncWithTermination(WIP, this, q);
      } else {
        discardWithTermination(WIP, this, q, currentContext());
      }

      if (canceled == NO_WORK_IN_PROGRESS_STATE) {
        actual.onError(Operators.onRejectedExecution(ree, s, error, buf, actual.currentContext()));
      }
    }
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
    if (sourceMode == SYNC) {
      drainSync();
    } else {
      drainRegular();
    }
  }

  void drainSync() {
    int missed = 1;

    final CoreSubscriber<? super Void> a = this.actual;
    final Queue<ByteBuf> q = queue;
    final WriterBuffer writer = this.writer;
    final int limit = this.limit;

    int e = NO_WORK_IN_PROGRESS_STATE;

    final ByteBuf cb = remainingBuffer;
    if (cb != null) {
      boolean sent;
      try {
        sent = trySend(writer, e, cb);
      } catch (Throwable ex) {
        remainingBuffer = null;
        cb.release();
        doError(a, Operators.onOperatorError(s, ex, actual.currentContext()));
        return;
      }

      if (!sent) {
        eventLoop.schedule(this);
        return;
      }

      remainingBuffer = null;

      e++;
      if (e == limit) {
        writer.commit();
        produced += e;
        e = NO_WORK_IN_PROGRESS_STATE;
      }
    }

    for (; ; ) {

      for (; ; ) {
        ByteBuf v;

        try {
          v = q.poll();
        } catch (Throwable ex) {
          doError(a, Operators.onOperatorError(s, ex, a.currentContext()));
          return;
        }

        if (canceled == 1) {
          RELEASER.accept(v);
          Operators.onDiscardQueueWithClear(q, currentContext(), null);
          return;
        }

        if (v == null) {
          doComplete(a);
          return;
        }

        boolean sent;
        try {
          sent = trySend(writer, e, v);
        } catch (Throwable ex) {
          v.release();
          doError(a, Operators.onOperatorError(s, ex, this.actual.currentContext()));
          return;
        }

        if (!sent) {
          break;
        }

        e++;

        if (e == limit) {
          writer.commit();
          produced += e;
          e = NO_WORK_IN_PROGRESS_STATE;
        }
      }

      if (canceled == 1) {
        Operators.onDiscardQueueWithClear(q, currentContext(), null);
        return;
      }

      if (q.isEmpty()) {
        doComplete(a);
        return;
      }

      int w = wip;
      if (missed == w) {
        produced += e;
        eventLoop.schedule(this);
        return;
      } else {
        missed = w;
      }
    }
  }

  void drainRegular() {
    int missed = 1;

    final CoreSubscriber<? super Void> a = actual;
    final Queue<ByteBuf> q = queue;
    final WriterBuffer writer = this.writer;
    final Subscription s = this.s;
    final int limit = this.limit;

    int e = NO_WORK_IN_PROGRESS_STATE;

    final ByteBuf cb = remainingBuffer;
    if (cb != null) {
      boolean sent;
      try {
        sent = trySend(writer, e, cb);
      } catch (Throwable ex) {
        ex.printStackTrace();
        cb.release();
        doError(a, Operators.onOperatorError(s, ex, a.currentContext()));
        return;
      }

      if (!sent) {
        eventLoop.schedule(this);
        return;
      }

      e++;
      if (e == limit) {
        writer.commit();
        produced += e;
        e = NO_WORK_IN_PROGRESS_STATE;
        System.err.println("request(" + limit + ")");
        s.request(limit);
      }
    }

    for (; ; ) {

      for (; ; ) {
        ByteBuf v;

        try {
          v = q.poll();
        } catch (Throwable ex) {
          doError(a, Operators.onOperatorError(s, ex, a.currentContext()));
          return;
        }

        if (canceled == 1) {
          RELEASER.accept(v);
          if (sourceMode == ASYNC) {
            discardAsyncWithTermination(WIP, this, q);
          } else {
            discardWithTermination(WIP, this, q, currentContext());
          }
          return;
        }

        boolean empty = v == null;

        if (empty) {
          if (done) {
            Throwable t = error;
            if (t != null) {
              doError(a, t);
            } else {
              doComplete(a);
            }
            return;
          }

          break;
        }

        boolean sent;
        try {
          sent = trySend(writer, e, v);
        } catch (Throwable ex) {
          v.release();
          doError(a, Operators.onOperatorError(s, ex, a.currentContext()));
          return;
        }

        if (!sent) {
          produced += e;
          eventLoop.schedule(this);
          return;
        }

        e++;

        if (e == limit) {
          writer.commit();
          produced += e;
          e = NO_WORK_IN_PROGRESS_STATE;
          System.err.println("request(" + limit + ")");
          s.request(limit);
        }
      }

      if (canceled == 1) {
        discardWithTermination(WIP, this, q, currentContext());
        return;
      }

      int w = wip;
      if (missed == w) {
        produced += e;
        missed = WIP.addAndGet(this, -missed);
        if (missed == NO_WORK_IN_PROGRESS_STATE) {
          break;
        }
      } else {
        missed = w;
      }
    }
  }

  boolean trySend(WriterBuffer writer, int e, ByteBuf byteBuf) {
    boolean flushed = false;
    boolean sent = false;
    for (; ; ) {
      final int size = byteBuf.readableBytes();
      int state = writer.claim(size);
      if (state > 0) {
        writer.write(byteBuf);
        byteBuf.release();
        sent = true;
      } else {
        if (e > 0 && !flushed) {
          writer.commit();
          flushed = true;
          continue;
        }
        remainingBuffer = byteBuf;
      }
      break;
    }
    return sent;
  }
}
