package io.rsocket.transport.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.Frame;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

class SendPublisher<V extends ReferenceCounted> extends Flux<Frame> {

  private static final AtomicIntegerFieldUpdater<SendPublisher> WIP =
      AtomicIntegerFieldUpdater.newUpdater(SendPublisher.class, "wip");

  private static final int MAX_SIZE = Queues.SMALL_BUFFER_SIZE;
  private static final int REFILL_SIZE = MAX_SIZE / 2;
  private static final AtomicReferenceFieldUpdater<SendPublisher, Object>
      INNER_SUBSCRIBER =
          AtomicReferenceFieldUpdater.newUpdater(
              SendPublisher.class, Object.class, "innerSubscriber");
  private final Publisher<Frame> source;
  private final Channel channel;
  private final EventLoop eventLoop;
  private final Queue<V> queue;
  private final AtomicBoolean terminated = new AtomicBoolean();
  private final AtomicBoolean completed = new AtomicBoolean();
  private final Function<Frame, V> transformer;
  private final SizeOf<V> sizeOf;
  
  private int pending;
  @SuppressWarnings("unused")
  private volatile int wip;

  @SuppressWarnings("unused")
  private volatile Object innerSubscriber;

  private long requested;

  private long requestedUpstream = MAX_SIZE;

  @SuppressWarnings("unchecked")
  SendPublisher(
      Publisher<Frame> source, Channel channel, Function<Frame, V> transformer, SizeOf<V> sizeOf) {
    this.source = source;
    this.channel = channel;
    this.queue = Queues.<V>small().get();
    this.eventLoop = channel.eventLoop();
    this.transformer = transformer;
    this.sizeOf = sizeOf;
  }

  private ChannelPromise writeCleanupPromise(V poll) {
    return channel
        .newPromise()
        .addListener(
            future -> {
              try {
                if (requested != Long.MAX_VALUE) {
                  requested--;
                }
                requestedUpstream--;
                pending--;

                InnerSubscriber is = (InnerSubscriber) INNER_SUBSCRIBER.get(SendPublisher.this);
                if (is != null) {
                  is.tryRequestMoreUpstream();
                  tryComplete(is);
                }
              } finally {
                ReferenceCountUtil.safeRelease(poll);
              }
            });
  }

  private void tryComplete(InnerSubscriber is) {
    if (pending == 0
        && completed.get()
        && queue.isEmpty()
        && !terminated.get()
        && !is.pendingFlush.get()) {
      terminated.set(true);
      is.destination.onComplete();
    }
  }

  @Override
  public void subscribe(CoreSubscriber<? super Frame> destination) {
    InnerSubscriber innerSubscriber = new InnerSubscriber(destination);
    if (!INNER_SUBSCRIBER.compareAndSet(this, null, innerSubscriber)) {
      throw new IllegalStateException("SendPublisher only allows one subscription");
    }

    InnerSubscription innerSubscription = new InnerSubscription(innerSubscriber);
    destination.onSubscribe(innerSubscription);
    source.subscribe(innerSubscriber);
  }

  @FunctionalInterface
  interface SizeOf<V> {
    int size(V v);
  }

  private class InnerSubscriber implements Subscriber<Frame> {
    final CoreSubscriber<? super Frame> destination;
    volatile Subscription s;
    private AtomicBoolean pendingFlush = new AtomicBoolean();
    private SendPublisher sendPublisher;

    private InnerSubscriber(CoreSubscriber<? super Frame> destination) {
      this.destination = destination;
    }

    @Override
    public void onSubscribe(Subscription s) {
      this.s = s;
      s.request(MAX_SIZE);
      tryDrain();
    }

    @Override
    public void onNext(Frame t) {
      if (!terminated.get()) {
        if (!queue.offer(transformer.apply(t))) {
          throw new IllegalStateException("missing back pressure");
        }
        tryDrain();
      }
    }

    @Override
    public void onError(Throwable t) {
      if (terminated.compareAndSet(false, true)) {
        try {
          s.cancel();
          destination.onError(t);
        } finally {
          if (!queue.isEmpty()) {
            queue.forEach(ReferenceCountUtil::safeRelease);
          }
        }
      }
    }

    @Override
    public void onComplete() {
      if (completed.compareAndSet(false, true)) {
        tryDrain();
      }
    }

    private void tryRequestMoreUpstream() {
      if (requestedUpstream <= REFILL_SIZE && s != null) {
        long u = MAX_SIZE - requestedUpstream;
        requestedUpstream = Operators.addCap(requestedUpstream, u);
        s.request(u);
      }
    }

    private void flush() {
      try {
        channel.flush();
        pendingFlush.set(false);
        tryComplete(this);
      } catch (Throwable t) {
        onError(t);
      }
    }

    private void tryDrain() {
      if (wip == 0 && !terminated.get() && WIP.getAndIncrement(SendPublisher.this) == 0) {
        try {
          if (eventLoop.inEventLoop()) {
            drain();
          } else {
            eventLoop.execute(this::drain);
          }
        } catch (Throwable t) {
          onError(t);
        }
      }
    }

    private void drain() {
      try {
        boolean scheduleFlush;
        int missed = 1;
        for (; ; ) {
          scheduleFlush = false;

          long r = Math.min(requested, requestedUpstream);
          while (r-- > 0) {
            V poll = queue.poll();
            if (poll != null && !terminated.get()) {
              int readableBytes = sizeOf.size(poll);
              pending++;
              if (channel.isWritable() && readableBytes <= channel.bytesBeforeUnwritable()) {
                channel.write(poll, writeCleanupPromise(poll));
                scheduleFlush = true;
              } else {
                scheduleFlush = false;
                channel.writeAndFlush(poll, writeCleanupPromise(poll));
              }

              tryRequestMoreUpstream();
            } else {
              break;
            }
          }

          if (scheduleFlush) {
            pendingFlush.set(true);
            eventLoop.execute(this::flush);
          }

          if (terminated.get()) {
            break;
          }

          missed = WIP.addAndGet(SendPublisher.this, -missed);
          if (missed == 0) {
            break;
          }
        }
      } catch (Throwable t) {
        onError(t);
      }
    }
  }

  private class InnerSubscription implements Subscription {
    private final InnerSubscriber innerSubscriber;

    private InnerSubscription(InnerSubscriber innerSubscriber) {
      this.innerSubscriber = innerSubscriber;
    }

    @Override
    public void request(long n) {
      if (eventLoop.inEventLoop()) {
        requested = Operators.addCap(n, requested);
        innerSubscriber.tryDrain();
      } else {
        eventLoop.execute(() -> request(n));
      }
    }

    @Override
    public void cancel() {
      terminated.set(true);
    }
  }
}
