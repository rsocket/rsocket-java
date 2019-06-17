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

public class Leases<T extends LeaseStats> {
  private static final Function<?, Flux<Lease>> noopLeaseSender = leaseStats -> Flux.never();
  private static final Consumer<Flux<Lease>> noopLeaseReceiver = leases -> {};

  private Function<?, Flux<Lease>> leaseSender = noopLeaseSender;
  private Consumer<Flux<Lease>> leaseReceiver = noopLeaseReceiver;
  private Optional<T> stats = Optional.empty();

  public static <T extends LeaseStats> Leases<T> create() {
    return new Leases<>();
  }

  public Leases<T> sender(Function<Optional<T>, Flux<Lease>> leaseSender) {
    this.leaseSender = leaseSender;
    return this;
  }

  public Leases<T> receiver(Consumer<Flux<Lease>> leaseReceiver) {
    this.leaseReceiver = leaseReceiver;
    return this;
  }

  public Leases<T> stats(T stats) {
    this.stats = Optional.of(Objects.requireNonNull(stats));
    return this;
  }

  @SuppressWarnings("unchecked")
  public Function<Optional<LeaseStats>, Flux<Lease>> sender() {
    return (Function<Optional<LeaseStats>, Flux<Lease>>) leaseSender;
  }

  public Consumer<Flux<Lease>> receiver() {
    return leaseReceiver;
  }

  @SuppressWarnings("unchecked")
  public Optional<LeaseStats> stats() {
    return (Optional<LeaseStats>) stats;
  }
}
