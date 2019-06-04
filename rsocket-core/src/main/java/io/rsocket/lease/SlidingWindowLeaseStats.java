/*
 * Copyright 2015-2019 the original author or authors.
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

package io.rsocket.lease;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;

/**
 * Statistics of responder lease represented as rolling set of sliding windows sampled at given
 * intervals
 */
public class SlidingWindowLeaseStats {
  private final AtomicBoolean isStarted = new AtomicBoolean();
  private volatile Lease lease = LeaseImpl.empty();
  private volatile Disposable nextWindowDisposable;
  private volatile CircularBuffer<SlidingWindow> windows;
  private volatile long startMillis;

  /** @return lease these stats are associated with */
  public Lease lease() {
    return lease;
  }

  /**
   * Start gathering new statistics with given window duration and count. Must be in stopped state
   *
   * @param lease target for this stats. Must be non null, may be empty
   * @param windowMillis window duration in millis. Must be positive
   * @param windowCount number of windows to maintain. Must be positive
   * @return this SlidingWindowLeaseStats
   */
  public SlidingWindowLeaseStats start(Lease lease, long windowMillis, int windowCount) {
    if (isStarted.compareAndSet(false, true)) {
      requirePositive(windowMillis);
      requirePositive(windowCount);
      this.lease = Objects.requireNonNull(lease, "lease");
      this.startMillis = System.currentTimeMillis();
      if (!lease.isEmpty()) {
        CircularBuffer<SlidingWindow> w = this.windows;
        if (w != null && w.maxSize() == windowCount) {
          w.clear();
        } else {
          this.windows = new CircularBuffer<>(windowCount);
        }
        this.nextWindowDisposable =
            Flux.interval(Duration.ofMillis(windowMillis))
                .doOnCancel(() -> addNextWindow())
                .doOnNext(ignored -> addNextWindow())
                .subscribe();
      } else {
        this.nextWindowDisposable = Disposables.disposed();
      }
      return this;
    } else {
      throw new IllegalStateException("Already started");
    }
  }

  /**
   * Stop gathering new statistics with given window duration and count. Must be in started state
   *
   * @return this SlidingWindowLeaseStats
   */
  public SlidingWindowLeaseStats stop() {
    if (isStarted.compareAndSet(true, false)) {
      nextWindowDisposable.dispose();
      return this;
    } else {
      throw new IllegalStateException("Already stopped");
    }
  }

  /**
   * @param index index of sliding window from 0 to {@link #size() - 1}. 0 corresponds to most
   *     recent window
   * @return sliding window associated with given index
   */
  public SlidingWindow window(int index) {
    if (isEmpty()) {
      throw new IllegalArgumentException("Stats are empty");
    }
    return windows.get(index);
  }

  /** @return most recent sliding window of lease */
  public SlidingWindow window() {
    return window(0);
  }

  /** @return true if no leases were sent yet, false otherwise */
  public boolean isEmpty() {
    return size() == 0;
  }

  /** @return number of sliding windows for this Lease. */
  public int size() {
    CircularBuffer<SlidingWindow> c = this.windows;
    return c == null ? 0 : c.size();
  }

  private void addNextWindow() {
    Lease l = this.lease;
    SlidingWindow next =
        isEmpty()
            ? SlidingWindow.first(l.getSuccessfulRequests(), l.getRejectedRequests(), startMillis)
            : windows.get(0).next(l.getSuccessfulRequests(), l.getRejectedRequests());
    windows.offer(next);
  }

  private static void requirePositive(long arg) {
    if (arg <= 0) {
      throw new IllegalArgumentException("argument must be positive: " + arg);
    }
  }

  private static void requirePositive(int arg) {
    requirePositive((long) arg);
  }

  static class CircularBuffer<T> {
    private final Object[] arr;
    private int start;
    private int end;
    private int size;

    public CircularBuffer(int size) {
      this.arr = new Object[size];
    }

    public synchronized CircularBuffer<T> offer(T t) {
      Objects.requireNonNull(t, "element");
      if (end == start && size != 0) {
        start = moveIndex(start, 1);
      }
      size = Math.min(size + 1, arr.length);
      arr[end] = t;
      end = moveIndex(end, 1);
      return this;
    }

    public synchronized T get(int pos) {
      if (pos < 0) {
        throw new IllegalArgumentException("Position should be positive");
      }
      int size = this.size;
      if (pos >= size) {
        String msg = "Element does not exist: size: %d, position: %d";
        throw new IllegalArgumentException(String.format(msg, size, pos));
      }
      return (T) arr[moveIndex(start, size - 1 - pos)];
    }

    public synchronized int size() {
      return size;
    }

    public int maxSize() {
      return arr.length;
    }

    public synchronized void clear() {
      for (int i = 0; i < size; i++) {
        arr[i] = null;
      }
      start = 0;
      end = 0;
      size = 0;
    }

    private int moveIndex(int idx, int offset) {
      return (idx + offset) % arr.length;
    }
  }
}
