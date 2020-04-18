package io.rsocket.core;

import io.netty.util.ReferenceCountUtil;
import io.rsocket.Payload;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

final class CleanOnClearQueueDecorator extends AtomicBoolean implements Queue<Payload> {
  final Queue<Payload> delegate;

  CleanOnClearQueueDecorator(Queue<Payload> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void clear() {
    set(true);
    Payload p;
    while ((p = delegate.poll()) != null) {
      ReferenceCountUtil.safeRelease(p);
    }
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return delegate.contains(o);
  }

  @Override
  public Iterator<Payload> iterator() {
    return delegate.iterator();
  }

  @Override
  public Object[] toArray() {
    return delegate.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return delegate.toArray(a);
  }

  @Override
  public boolean add(Payload payload) {
    return delegate.add(payload);
  }

  @Override
  public boolean remove(Object o) {
    return delegate.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return delegate.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends Payload> c) {
    return delegate.addAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return delegate.retainAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return delegate.retainAll(c);
  }

  @Override
  public boolean offer(Payload payload) {
    if (get()) {
      ReferenceCountUtil.safeRelease(payload);
      return true;
    }
    return delegate.offer(payload);
  }

  @Override
  public Payload remove() {
    return delegate.remove();
  }

  @Override
  public Payload poll() {
    return delegate.poll();
  }

  @Override
  public Payload element() {
    return delegate.element();
  }

  @Override
  public Payload peek() {
    return delegate.peek();
  }
}
