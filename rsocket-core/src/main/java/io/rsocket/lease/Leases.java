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

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import reactor.core.publisher.Flux;

public class Leases<T extends RequestTracker> {
  private static final LeaseSender<?> noopLeaseSender = leaseTracker -> Flux.never();
  private static final LeaseReceiver noopLeaseReceiver = leases -> {};

  @SuppressWarnings("unchecked")
  private LeaseSender<T> leaseSender = (LeaseSender<T>) noopLeaseSender;

  private LeaseReceiver leaseReceiver = noopLeaseReceiver;
  private T leaseTracker;

  public static <T extends RequestTracker> Leases<T> create() {
    return new Leases<>();
  }

  public Leases<T> sender(LeaseSender<T> leaseSender) {
    this.leaseSender = leaseSender;
    return this;
  }

  public Leases<T> receiver(LeaseReceiver leaseReceiver) {
    this.leaseReceiver = leaseReceiver;
    return this;
  }

  public Leases<T> tracker(T tracker) {
    this.leaseTracker = Objects.requireNonNull(tracker);
    return this;
  }

  public T leaseTracker() {
    return leaseTracker;
  }

  public LeaseReceiver leaseReceiver() {
    return leaseReceiver;
  }

  public LeaseSender<T> leaseSender() {
    return leaseSender;
  }

  /** @deprecated in favor of {@link #sender(LeaseSender)} */
  @Deprecated
  @SuppressWarnings("unchecked")
  public <K extends LeaseStats> Leases<K> sender(Function<Optional<K>, Flux<Lease>> leaseSender) {
    this.leaseSender = leaseTracker -> leaseSender.apply(Optional.ofNullable((K) leaseTracker));
    return (Leases<K>) this;
  }

  /** @deprecated in favor of the {@link #receiver(LeaseReceiver)} */
  @Deprecated
  public Leases<T> receiver(Consumer<Flux<Lease>> leaseReceiver) {
    this.leaseReceiver = leaseReceiver::accept;
    return this;
  }

  /** @deprecated in favor of the {@link #tracker(RequestTracker)} method */
  @Deprecated
  public Leases<T> stats(T stats) {
    this.leaseTracker = Objects.requireNonNull(stats);
    return this;
  }

  /** @deprecated in favor of the {@link #leaseSender()} method */
  @Deprecated
  public Function<Optional<LeaseStats>, Flux<Lease>> sender() {
    return __ -> Flux.never();
  }

  /** @deprecated in favor of the {@link #leaseReceiver()} method */
  @Deprecated
  public Consumer<Flux<Lease>> receiver() {
    return (f) -> {};
  }

  /** @deprecated in favor of the {@link #leaseTracker()} method */
  @Deprecated
  public Optional<LeaseStats> stats() {
    return leaseTracker instanceof LeaseStats
        ? Optional.of((LeaseStats) leaseTracker)
        : Optional.empty();
  }
}
