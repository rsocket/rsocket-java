package io.rsocket.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.netty.FutureMono;
import reactor.util.concurrent.Queues;

import java.nio.channels.ClosedChannelException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

class SendPublisher<V extends ReferenceCounted> extends Flux<ByteBuf> {

  private static final AtomicIntegerFieldUpdater<SendPublisher> WIP =
      AtomicIntegerFieldUpdater.newUpdater(SendPublisher.class, "wip");

  private static final int MAX_SIZE = Queues.SMALL_BUFFER_SIZE;
  private static final int REFILL_SIZE = MAX_SIZE / 2;
  private static final AtomicReferenceFieldUpdater<SendPublisher, Object> INNER_SUBSCRIBER =
      AtomicReferenceFieldUpdater.newUpdater(SendPublisher.class, Object.class, "innerSubscriber");
  private static final AtomicIntegerFieldUpdater<SendPublisher> TERMINATED =
      AtomicIntegerFieldUpdater.newUpdater(SendPublisher.class, "terminated");
  private final Publisher<ByteBuf> source;
  private final Channel channel;
  private final EventLoop eventLoop;

  private final Queue<ByteBuf> queue;
  private final AtomicBoolean completed = new AtomicBoolean();
  private final Function<ByteBuf, V> transformer;
  private final SizeOf<V> sizeOf;

  @SuppressWarnings("unused")
  private volatile int terminated;

  private int pending;

  @SuppressWarnings("unused")
  private volatile int wip;

  @SuppressWarnings("unused")
  private volatile Object innerSubscriber;

  private long requested;

  private long requestedUpstream = MAX_SIZE;

  private boolean fuse;

  @SuppressWarnings("unchecked")
  SendPublisher(
      Publisher<ByteBuf> source,
      Channel channel,
      Function<ByteBuf, V> transformer,
      SizeOf<V> sizeOf) {
    this(Queues.<ByteBuf>small().get(), source, channel, transformer, sizeOf);
  }

  @SuppressWarnings("unchecked")
  SendPublisher(
      Queue<ByteBuf> queue,
      Publisher<ByteBuf> source,
      Channel channel,
      Function<ByteBuf, V> transformer,
      SizeOf<V> sizeOf) {
    this.source = source;
    this.channel = channel;
    this.queue = queue;
    this.eventLoop = channel.eventLoop();
    this.transformer = transformer;
    this.sizeOf = sizeOf;

    fuse = queue instanceof Fuseable.QueueSubscription;
  }

  private ChannelPromise writeCleanupPromise(V poll) {
    return channel
        .newPromise()
        .addListener(
            future -> {
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
              if (poll.refCnt() > 0) {
                ReferenceCountUtil.safeRelease(poll);
              }
            });
  }

  private void tryComplete(InnerSubscriber is) {
    if (pending == 0
        && completed.get()
        && queue.isEmpty()
        && terminated == 0
        && !is.pendingFlush.get()) {
      TERMINATED.set(SendPublisher.this, 1);
      is.destination.onComplete();
    }
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> destination) {
    InnerSubscriber innerSubscriber = new InnerSubscriber(destination);
    if (!INNER_SUBSCRIBER.compareAndSet(this, null, innerSubscriber)) {
      Operators.error(
          destination, new IllegalStateException("SendPublisher only allows one subscription"));
    } else {
      InnerSubscription innerSubscription = new InnerSubscription(innerSubscriber);
      destination.onSubscribe(innerSubscription);
      source.subscribe(innerSubscriber);
    }
  }

  @FunctionalInterface
  interface SizeOf<V> {
    int size(V v);
  }

  private class InnerSubscriber implements Subscriber<ByteBuf> {
    final CoreSubscriber<? super ByteBuf> destination;
    volatile Subscription s;
    private AtomicBoolean pendingFlush = new AtomicBoolean();

    private InnerSubscriber(CoreSubscriber<? super ByteBuf> destination) {
      this.destination = destination;
      FutureMono.from(channel.closeFuture())
          .doFinally(s -> onError(new ClosedChannelException()))
          .subscribe();
    }

    @Override
    public void onSubscribe(Subscription s) {
      this.s = s;
      s.request(MAX_SIZE);
      tryDrain();
    }

    @Override
    public void onNext(ByteBuf t) {
      if (terminated == 0) {
        if (!fuse && !queue.offer(t)) {
          throw new IllegalStateException("missing back pressure");
        }
        tryDrain();
      }
    }

    @Override
    public void onError(Throwable t) {
      if (TERMINATED.compareAndSet(SendPublisher.this, 0, 1)) {
        try {
          s.cancel();
          destination.onError(t);
        } finally {
          ByteBuf byteBuf = queue.poll();
          while (byteBuf != null) {
            ReferenceCountUtil.safeRelease(byteBuf);
            byteBuf = queue.poll();
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
      if (wip == 0 && terminated == 0 && WIP.getAndIncrement(SendPublisher.this) == 0) {
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
            ByteBuf ByteBuf = queue.poll();
            if (ByteBuf != null && terminated == 0) {
              V poll = transformer.apply(ByteBuf);
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

          if (terminated == 1) {
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
      TERMINATED.set(SendPublisher.this, 1);
      while (!queue.isEmpty()) {
        ByteBuf poll = queue.poll();
        if (poll != null) {
          ReferenceCountUtil.safeRelease(poll);
        }
      }
    }
  }
}
