package io.rsocket.addons;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

public class ResolvingRSocket implements RSocket {

  static final CancellationException ON_DISPOSE =
      new CancellationException("ResolvingRSocket has already been disposed");

  final Mono<? extends RSocket> source;
  final MonoProcessor<Void> onDispose;
  final ResolvingMainSubscriber subscriber;

  volatile int wip;

  static final AtomicIntegerFieldUpdater<ResolvingRSocket> WIP =
      AtomicIntegerFieldUpdater.newUpdater(ResolvingRSocket.class, "wip");

  volatile BiConsumer<RSocket, Throwable>[] subscribers;

  @SuppressWarnings("rawtypes")
  static final AtomicReferenceFieldUpdater<ResolvingRSocket, BiConsumer[]> SUBSCRIBERS =
      AtomicReferenceFieldUpdater.newUpdater(
          ResolvingRSocket.class, BiConsumer[].class, "subscribers");

  @SuppressWarnings("unchecked")
  static final BiConsumer<RSocket, Throwable>[] EMPTY_UNSUBSCRIBED = new BiConsumer[0];

  @SuppressWarnings("unchecked")
  static final BiConsumer<RSocket, Throwable>[] EMPTY_SUBSCRIBED = new BiConsumer[0];

  @SuppressWarnings("unchecked")
  static final BiConsumer<RSocket, Throwable>[] READY = new BiConsumer[0];

  @SuppressWarnings("unchecked")
  static final BiConsumer<RSocket, Throwable>[] TERMINATED = new BiConsumer[0];

  static final int ADDED_STATE = 0;
  static final int READY_STATE = 1;
  static final int TERMINATED_STATE = 2;

  RSocket value;
  Throwable t;

  public ResolvingRSocket(Mono<? extends RSocket> source) {
    this.source = source;
    this.subscriber = new ResolvingMainSubscriber(this);
    this.onDispose = MonoProcessor.create();

    SUBSCRIBERS.lazySet(this, EMPTY_UNSUBSCRIBED);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return new FlatMapInner<>(this, payload, FrameType.REQUEST_FNF);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return new FlatMapInner<>(this, payload, FrameType.REQUEST_RESPONSE);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return new FlatMapManyInner<>(this, payload, FrameType.REQUEST_STREAM);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new FlatMapManyInner<>(this, payloads, FrameType.REQUEST_CHANNEL);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return new FlatMapInner<>(this, payload, FrameType.METADATA_PUSH);
  }

  @Override
  public double availability() {
    if (this.subscribers == READY) {
      final RSocket rsocket = this.value;
      return rsocket.availability();
    } else {
      return 0.0d;
    }
  }

  @Override
  public void dispose() {
    this.terminate(ON_DISPOSE);
  }

  @Override
  public boolean isDisposed() {
    return this.subscribers == TERMINATED;
  }

  @Override
  public Mono<Void> onClose() {
    return onDispose;
  }

  void subscribe(BiConsumer<RSocket, Throwable> actual) {
    final int state = add(actual);

    if (state == READY_STATE) {
      actual.accept(value, null);
    } else if (state == TERMINATED_STATE) {
      actual.accept(null, t);
    }
  }

  @SuppressWarnings("unchecked")
  void terminate(Throwable t) {
    if (isDisposed()) {
      return;
    }

    // writes happens before volatile write
    this.t = t;

    final BiConsumer<RSocket, Throwable>[] subscribers = SUBSCRIBERS.getAndSet(this, TERMINATED);
    if (subscribers == TERMINATED) {
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    this.subscriber.dispose();

    this.doFinally();

    for (BiConsumer<RSocket, Throwable> consumer : subscribers) {
      consumer.accept(null, t);
    }
  }

  void complete() {
    BiConsumer<RSocket, Throwable>[] subscribers = this.subscribers;
    if (subscribers == TERMINATED) {
      return;
    }

    final RSocket value = this.value;

    for (; ; ) {
      // ensures TERMINATE is going to be replaced with READY
      if (SUBSCRIBERS.compareAndSet(this, subscribers, READY)) {
        break;
      }

      subscribers = this.subscribers;

      if (subscribers == TERMINATED) {
        this.doFinally();
        return;
      }
    }

    value.onClose().subscribe(null, __ -> invalidate(), this::invalidate);

    for (BiConsumer<RSocket, Throwable> consumer : subscribers) {
      consumer.accept(value, null);
    }
  }

  void doFinally() {
    if (WIP.getAndIncrement(this) != 0) {
      return;
    }

    int m = 1;
    RSocket value;

    for (; ; ) {
      value = this.value;

      if (value != null && isDisposed()) {
        this.value = null;
        value.dispose();
        if (t == ON_DISPOSE) {
          onDispose.onComplete();
        } else {
          onDispose.onError(t);
        }
        return;
      }

      m = WIP.addAndGet(this, -m);
      if (m == 0) {
        return;
      }
    }
  }

  // Check RSocket is not good
  void invalidate() {
    if (this.subscribers == TERMINATED) {
      return;
    }

    final BiConsumer<RSocket, Throwable>[] subscribers = this.subscribers;

    if (subscribers == READY && SUBSCRIBERS.compareAndSet(this, READY, EMPTY_UNSUBSCRIBED)) {
      final RSocket value = this.value;
      this.value = null;

      if (value != null) {
        value.dispose();
      }
    }
  }

  int add(BiConsumer<RSocket, Throwable> ps) {
    for (; ; ) {
      BiConsumer<RSocket, Throwable>[] a = this.subscribers;

      if (a == TERMINATED) {
        return TERMINATED_STATE;
      }

      if (a == READY) {
        return READY_STATE;
      }

      int n = a.length;
      @SuppressWarnings("unchecked")
      BiConsumer<RSocket, Throwable>[] b = new BiConsumer[n + 1];
      System.arraycopy(a, 0, b, 0, n);
      b[n] = ps;

      if (SUBSCRIBERS.compareAndSet(this, a, b)) {
        if (a == EMPTY_UNSUBSCRIBED) {
          this.source.subscribe(this.subscriber);
        }
        return ADDED_STATE;
      }
    }
  }

  @SuppressWarnings("unchecked")
  void remove(BiConsumer<RSocket, Throwable> ps) {
    for (; ; ) {
      BiConsumer<RSocket, Throwable>[] a = this.subscribers;
      int n = a.length;
      if (n == 0) {
        return;
      }

      int j = -1;
      for (int i = 0; i < n; i++) {
        if (a[i] == ps) {
          j = i;
          break;
        }
      }

      if (j < 0) {
        return;
      }

      BiConsumer<RSocket, Throwable>[] b;

      if (n == 1) {
        b = EMPTY_SUBSCRIBED;
      } else {
        b = new BiConsumer[n - 1];
        System.arraycopy(a, 0, b, 0, j);
        System.arraycopy(a, j + 1, b, j, n - j - 1);
      }
      if (SUBSCRIBERS.compareAndSet(this, a, b)) {
        return;
      }
    }
  }

  static final class ResolvingMainSubscriber implements CoreSubscriber<RSocket> {

    final ResolvingRSocket parent;

    volatile Subscription s;

    static final AtomicReferenceFieldUpdater<ResolvingMainSubscriber, Subscription> S =
        AtomicReferenceFieldUpdater.newUpdater(
            ResolvingMainSubscriber.class, Subscription.class, "s");

    ResolvingMainSubscriber(ResolvingRSocket parent) {
      this.parent = parent;
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (Operators.setOnce(S, this, s)) {
        s.request(Long.MAX_VALUE);
      }
    }

    @Override
    public void onComplete() {
      final Subscription s = this.s;
      final ResolvingRSocket p = this.parent;
      final RSocket value = p.value;

      if (s == Operators.cancelledSubscription() || !S.compareAndSet(this, s, null)) {
        p.doFinally();
        return;
      }

      if (value == null) {
        p.terminate(new IllegalStateException("Unexpected Completion of the Upstream"));
      } else {
        p.complete();
      }
    }

    @Override
    public void onError(Throwable t) {
      final Subscription s = this.s;
      final ResolvingRSocket p = this.parent;

      if (s == Operators.cancelledSubscription()
          || S.getAndSet(this, Operators.cancelledSubscription())
              == Operators.cancelledSubscription()) {
        p.doFinally();
        Operators.onErrorDropped(t, Context.empty());
        return;
      }

      // terminate upstream which means retryBackoff has exhausted
      p.terminate(t);
    }

    @Override
    public void onNext(RSocket value) {
      if (this.s == Operators.cancelledSubscription()) {
        value.dispose();
        return;
      }

      final ResolvingRSocket p = this.parent;

      p.value = value;
      // volatile write and check on racing
      p.doFinally();
    }

    void dispose() {
      Operators.terminate(S, this);
    }
  }

  static final class FlatMapInner<T> extends Mono<T>
      implements CoreSubscriber<T>, Subscription, Scannable, BiConsumer<RSocket, Throwable> {

    final ResolvingRSocket parent;
    final FrameType interactionType;
    final Payload payload;

    volatile long requested;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<FlatMapInner> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(FlatMapInner.class, "requested");

    static final long STATE_UNSUBSCRIBED = -1;
    static final long STATE_SUBSCRIBER_SET = 0;
    static final long STATE_SUBSCRIBED = -2;
    static final long STATE_CANCELLED = Long.MIN_VALUE;

    Subscription s;
    CoreSubscriber<? super T> actual;
    boolean done;

    FlatMapInner(ResolvingRSocket parent, Payload payload, FrameType interactionType) {
      this.parent = parent;
      this.payload = payload;
      this.interactionType = interactionType;

      REQUESTED.lazySet(this, STATE_UNSUBSCRIBED);
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
      if (this.requested == STATE_UNSUBSCRIBED
          && REQUESTED.compareAndSet(this, STATE_UNSUBSCRIBED, STATE_SUBSCRIBER_SET)) {

        actual.onSubscribe(this);

        if (this.requested == STATE_CANCELLED) {
          return;
        }

        this.actual = actual;
        this.parent.subscribe(this);
      } else {
        Operators.error(actual, new IllegalStateException("Only a single Subscriber allowed"));
      }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(RSocket rSocket, Throwable t) {
      if (this.requested == STATE_CANCELLED) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.payload);
        onError(t);
        return;
      }

      Mono<?> source;
      switch (interactionType) {
        case REQUEST_FNF:
          source = rSocket.fireAndForget(this.payload);
          break;
        case REQUEST_RESPONSE:
          source = rSocket.requestResponse(this.payload);
          break;
        case METADATA_PUSH:
          source = rSocket.metadataPush(this.payload);
          break;
        default:
          Operators.error(this.actual, new IllegalStateException("Should never happen"));
          return;
      }

      source.subscribe((CoreSubscriber) this);
    }

    @Override
    public Context currentContext() {
      return this.actual.currentContext();
    }

    @Nullable
    @Override
    public Object scanUnsafe(Attr key) {
      long state = this.requested;

      if (key == Attr.PARENT) {
        return this.s;
      }
      if (key == Attr.ACTUAL) {
        return this.parent;
      }
      if (key == Attr.TERMINATED) {
        return this.done;
      }
      if (key == Attr.CANCELLED) {
        return state == STATE_CANCELLED;
      }

      return null;
    }

    @Override
    public void onSubscribe(Subscription s) {
      final long state = this.requested;
      Subscription a = this.s;
      if (state == STATE_CANCELLED) {
        s.cancel();
        return;
      }
      if (a != null) {
        s.cancel();
        return;
      }

      long r;
      long accumulated = 0;
      for (; ; ) {
        r = this.requested;

        if (r == STATE_CANCELLED || r == STATE_SUBSCRIBED) {
          s.cancel();
          return;
        }

        this.s = s;

        long toRequest = r - accumulated;
        if (toRequest > 0) { // if there is something,
          s.request(toRequest); // then we do a request on the given subscription
        }
        accumulated = r;

        if (REQUESTED.compareAndSet(this, r, STATE_SUBSCRIBED)) {
          return;
        }
      }
    }

    @Override
    public void onNext(T payload) {
      this.actual.onNext(payload);
    }

    @Override
    public void onError(Throwable t) {
      if (this.done) {
        Operators.onErrorDropped(t, this.actual.currentContext());
        return;
      }

      this.done = true;
      this.actual.onError(t);
    }

    @Override
    public void onComplete() {
      if (this.done) {
        return;
      }

      this.done = true;
      this.actual.onComplete();
    }

    @Override
    public void request(long n) {
      if (Operators.validate(n)) {
        long r = this.requested; // volatile read beforehand

        if (r > STATE_SUBSCRIBED) { // works only in case onSubscribe has not happened
          long u;
          for (; ; ) { // normal CAS loop with overflow protection
            if (r == Long.MAX_VALUE) {
              // if r == Long.MAX_VALUE then we dont care and we can loose this
              // request just in case of racing
              return;
            }
            u = Operators.addCap(r, n);
            if (REQUESTED.compareAndSet(this, r, u)) {
              // Means increment happened before onSubscribe
              return;
            } else {
              // Means increment happened after onSubscribe

              // update new state to see what exactly happened (onSubscribe |cancel | requestN)
              r = this.requested;

              // check state (expect -1 | -2 to exit, otherwise repeat)
              if (r < 0) {
                break;
              }
            }
          }
        }

        if (r == STATE_CANCELLED) { // if canceled, just exit
          return;
        }

        // if onSubscribe -> subscription exists (and we sure of that because volatile read
        // after volatile write) so we can execute requestN on the subscription
        this.s.request(n);
      }
    }

    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
      if (state == STATE_CANCELLED) {
        return;
      }

      if (state == STATE_SUBSCRIBED) {
        this.s.cancel();
      } else {
        this.parent.remove(this);
        ReferenceCountUtil.safeRelease(this.payload);
      }
    }
  }

  static final class FlatMapManyInner<T> extends Flux<Payload>
      implements CoreSubscriber<Payload>, BiConsumer<RSocket, Throwable>, Subscription, Scannable {

    final ResolvingRSocket parent;
    final FrameType interactionType;
    final T fluxOrPayload;

    volatile long requested;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<FlatMapManyInner> REQUESTED =
        AtomicLongFieldUpdater.newUpdater(FlatMapManyInner.class, "requested");

    static final long STATE_UNSUBSCRIBED = -1;
    static final long STATE_SUBSCRIBER_SET = 0;
    static final long STATE_SUBSCRIBED = -2;
    static final long STATE_CANCELLED = Long.MIN_VALUE;

    Subscription s;
    CoreSubscriber<? super Payload> actual;
    boolean done;

    FlatMapManyInner(ResolvingRSocket parent, T fluxOrPayload, FrameType interactionType) {
      this.parent = parent;
      this.fluxOrPayload = fluxOrPayload;
      this.interactionType = interactionType;

      REQUESTED.lazySet(this, STATE_UNSUBSCRIBED);
    }

    @Override
    public void subscribe(CoreSubscriber<? super Payload> actual) {
      if (this.requested == STATE_UNSUBSCRIBED
          && REQUESTED.compareAndSet(this, STATE_UNSUBSCRIBED, STATE_SUBSCRIBER_SET)) {

        actual.onSubscribe(this);

        if (this.requested == STATE_CANCELLED) {
          return;
        }

        this.actual = actual;
        this.parent.subscribe(this);
      } else {
        Operators.error(actual, new IllegalStateException("Only a single Subscriber allowed"));
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accept(RSocket rSocket, Throwable t) {
      if (this.requested == STATE_CANCELLED) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.fluxOrPayload);
        onError(t);
        return;
      }

      Flux<? extends Payload> source;
      switch (this.interactionType) {
        case REQUEST_STREAM:
          source = rSocket.requestStream((Payload) this.fluxOrPayload);
          break;
        case REQUEST_CHANNEL:
          source = rSocket.requestChannel((Flux<Payload>) this.fluxOrPayload);
          break;
        default:
          Operators.error(this.actual, new IllegalStateException("Should never happen"));
          return;
      }

      source.subscribe(this);
    }

    @Override
    public Context currentContext() {
      return this.actual.currentContext();
    }

    @Nullable
    @Override
    public Object scanUnsafe(Attr key) {
      long state = this.requested;

      if (key == Attr.PARENT) {
        return this.s;
      }
      if (key == Attr.ACTUAL) {
        return this.parent;
      }
      if (key == Attr.TERMINATED) {
        return this.done;
      }
      if (key == Attr.CANCELLED) {
        return state == STATE_CANCELLED;
      }

      return null;
    }

    @Override
    public void onSubscribe(Subscription s) {
      final long state = this.requested;
      Subscription a = this.s;
      if (state == STATE_CANCELLED) {
        s.cancel();
        return;
      }
      if (a != null) {
        s.cancel();
        return;
      }

      long r;
      long accumulated = 0;
      for (; ; ) {
        r = this.requested;

        if (r == STATE_CANCELLED || r == STATE_SUBSCRIBED) {
          s.cancel();
          return;
        }

        this.s = s;

        long toRequest = r - accumulated;
        if (toRequest > 0) { // if there is something,
          s.request(toRequest); // then we do a request on the given subscription
        }
        accumulated = r;

        if (REQUESTED.compareAndSet(this, r, STATE_SUBSCRIBED)) {
          return;
        }
      }
    }

    @Override
    public void onNext(Payload payload) {
      this.actual.onNext(payload);
    }

    @Override
    public void onError(Throwable t) {
      if (this.done) {
        Operators.onErrorDropped(t, this.actual.currentContext());
        return;
      }

      this.done = true;
      this.actual.onError(t);
    }

    @Override
    public void onComplete() {
      if (this.done) {
        return;
      }

      this.done = true;
      this.actual.onComplete();
    }

    @Override
    public void request(long n) {
      if (Operators.validate(n)) {
        long r = this.requested; // volatile read beforehand

        if (r > STATE_SUBSCRIBED) { // works only in case onSubscribe has not happened
          long u;
          for (; ; ) { // normal CAS loop with overflow protection
            if (r == Long.MAX_VALUE) {
              // if r == Long.MAX_VALUE then we dont care and we can loose this
              // request just in case of racing
              return;
            }
            u = Operators.addCap(r, n);
            if (REQUESTED.compareAndSet(this, r, u)) {
              // Means increment happened before onSubscribe
              return;
            } else {
              // Means increment happened after onSubscribe

              // update new state to see what exactly happened (onSubscribe |cancel | requestN)
              r = this.requested;

              // check state (expect -1 | -2 to exit, otherwise repeat)
              if (r < 0) {
                break;
              }
            }
          }
        }

        if (r == STATE_CANCELLED) { // if canceled, just exit
          return;
        }

        // if onSubscribe -> subscription exists (and we sure of that because volatile read
        // after volatile write) so we can execute requestN on the subscription
        this.s.request(n);
      }
    }

    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
      if (state == STATE_CANCELLED) {
        return;
      }

      if (state == STATE_SUBSCRIBED) {
        this.s.cancel();
      } else {
        this.parent.remove(this);
        ReferenceCountUtil.safeRelease(this.fluxOrPayload);
      }
    }
  }
}
