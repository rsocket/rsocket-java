package io.rsocket.transport.aeron;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import reactor.core.Disposable;

public final class EventLoopGroup extends AtomicInteger implements Disposable {

  static AtomicReference<EventLoopGroup> CACHED_DEFAULT = new AtomicReference<>();

  static final int DISPOSED = Integer.MIN_VALUE;

  final EventLoop[] resources;

  EventLoopGroup(int size, Supplier<? extends IdleStrategy> idleStrategySupplier) {
    this.resources = new EventLoop[size];

    for (int i = 0; i < size; i++) {
      final IdleStrategy idleStrategy = idleStrategySupplier.get();
      final EventLoop eventLoop = new EventLoop(idleStrategy);
      this.resources[i] = eventLoop;
      new Thread(eventLoop).start();
    }
  }

  @Override
  public void dispose() {
    if (get() == DISPOSED && getAndSet(DISPOSED) == DISPOSED) {
      return;
    }

    for (EventLoop resource : this.resources) {
      resource.dispose();
    }
  }

  @Override
  public boolean isDisposed() {
    return get() == DISPOSED;
  }

  EventLoop next() {
    final EventLoop[] resources = this.resources;
    final int resourcesCount = resources.length;
    for (; ; ) {
      final int index = get();

      if (index == DISPOSED) {
        return resources[0];
      }

      final int nextIndex = (index + 1) % resourcesCount;
      if (compareAndSet(index, nextIndex)) {
        return resources[index];
      }
    }
  }

  public static EventLoopGroup create(int size) {
    return create(size, () -> new BackoffIdleStrategy(100, 1000, 10000, 100000));
  }

  public static EventLoopGroup create(
      int size, Supplier<? extends IdleStrategy> idleStrategySupplier) {
    return new EventLoopGroup(size, idleStrategySupplier);
  }

  public static EventLoopGroup cached() {
    EventLoopGroup s = CACHED_DEFAULT.get();
    if (s != null) {
      return s;
    }
    s =
        new EventLoopGroup(
            Runtime.getRuntime().availableProcessors(),
            () -> new BackoffIdleStrategy(100, 1000, 10000, 100000));
    if (CACHED_DEFAULT.compareAndSet(null, s)) {
      return s;
    }
    // the reference was updated in the meantime with a cached scheduler
    // fallback to it and dispose the extraneous one
    s.dispose();
    return CACHED_DEFAULT.get();
  }
}
