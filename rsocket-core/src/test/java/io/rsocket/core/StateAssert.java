/*
 * Copyright 2015-2020 the original author or authors.
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
package io.rsocket.core;

import static io.rsocket.core.ShouldHaveFlag.*;
import static io.rsocket.core.ShouldNotHaveFlag.shouldNotHaveFlag;
import static io.rsocket.core.StateUtils.*;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.internal.Failures;

public class StateAssert<T> extends AbstractAssert<StateAssert<T>, AtomicLongFieldUpdater<T>> {

  public static <T> StateAssert<T> assertThat(AtomicLongFieldUpdater<T> updater, T instance) {
    return new StateAssert<>(updater, instance, () -> 0L);
  }

  public static StateAssert<FireAndForgetRequesterMono> assertThat(
      FireAndForgetRequesterMono instance) {
    return new StateAssert<>(FireAndForgetRequesterMono.STATE, instance, () -> 0L);
  }

  public static StateAssert<RequestResponseRequesterMono> assertThat(
      RequestResponseRequesterMono instance) {
    return new StateAssert<>(RequestResponseRequesterMono.STATE, instance, () -> 0L);
  }

  public static StateAssert<RequestStreamRequesterFlux> assertThat(
      RequestStreamRequesterFlux instance) {
    return new StateAssert<>(RequestStreamRequesterFlux.STATE, instance, () -> instance.requested);
  }

  public static StateAssert<RequestChannelRequesterFlux> assertThat(
      RequestChannelRequesterFlux instance) {
    return new StateAssert<>(RequestChannelRequesterFlux.STATE, instance, () -> instance.requested);
  }

  public static StateAssert<RequestChannelResponderSubscriber> assertThat(
      RequestChannelResponderSubscriber instance) {
    return new StateAssert<>(RequestChannelResponderSubscriber.STATE, instance, () -> instance.requested);
  }

  private final Failures failures = Failures.instance();
  private final T instance;
  private final LongSupplier requestNSupplier;

  public StateAssert(AtomicLongFieldUpdater<T> updater, T instance, LongSupplier requestNSupplier) {
    super(updater, StateAssert.class);
    this.instance = instance;
    this.requestNSupplier = requestNSupplier;
  }

  public StateAssert<T> isUnsubscribed() {
    long currentState = actual.get(instance);
    if (isSubscribed(currentState) || StateUtils.isTerminated(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, UNSUBSCRIBED_STATE));
    }
    return this;
  }

  public StateAssert<T> hasSubscribedFlagOnly() {
    long currentState = actual.get(instance);
    if (currentState != SUBSCRIBED_FLAG) {
      throw failures.failure(info, shouldHaveFlag(currentState, SUBSCRIBED_FLAG));
    }
    return this;
  }

  public StateAssert<T> hasSubscribedFlag() {
    long currentState = actual.get(instance);
    if (!isSubscribed(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, SUBSCRIBED_FLAG));
    }
    return this;
  }

  public StateAssert<T> hasRequestedTimes(int n) {
    long currentState = actual.get(instance);
    if (requestedTimes(currentState) != n) {
      throw failures.failure(info, shouldHaveRequestN(currentState, n));
    }
    return this;
  }

  public StateAssert<T> hasRequestN(long n) {
    final long requestN = requestNSupplier.getAsLong();
    if (requestN != n) {
      throw failures.failure(info, shouldHaveRequestN(requestN, n));
    }
    return this;
  }

  public StateAssert<T> hasRequestNBetween(long min, long max) {
    final long requestN = requestNSupplier.getAsLong();
    if (requestN < min || requestN > max) {
      throw failures.failure(info, shouldHaveRequestNBetween(requestN, min, max));
    }
    return this;
  }

  public StateAssert<T> hasFirstFrameSentFlag() {
    long currentState = actual.get(instance);
    if (!isFirstFrameSent(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, FIRST_FRAME_SENT_FLAG));
    }
    return this;
  }

  public StateAssert<T> hasNoFirstFrameSentFlag() {
    long currentState = actual.get(instance);
    if (isFirstFrameSent(currentState)) {
      throw failures.failure(info, shouldNotHaveFlag(currentState, FIRST_FRAME_SENT_FLAG));
    }
    return this;
  }

  public StateAssert<T> hasReassemblingFlag() {
    long currentState = actual.get(instance);
    if (!isReassembling(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, REASSEMBLING_FLAG));
    }
    return this;
  }

  public StateAssert<T> hasNoReassemblingFlag() {
    long currentState = actual.get(instance);
    if (isReassembling(currentState)) {
      throw failures.failure(info, shouldNotHaveFlag(currentState, REASSEMBLING_FLAG));
    }
    return this;
  }

  public StateAssert<T> hasInboundTerminated() {
    long currentState = actual.get(instance);
    if (!StateUtils.isInboundTerminated(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, INBOUND_TERMINATED_FLAG));
    }
    return this;
  }

  public StateAssert<T> hasOutboundTerminated() {
    long currentState = actual.get(instance);
    if (!StateUtils.isOutboundTerminated(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, OUTBOUND_TERMINATED_FLAG));
    }
    return this;
  }

  public StateAssert<T> isTerminated() {
    long currentState = actual.get(instance);
    if (!StateUtils.isTerminated(currentState)) {
      throw failures.failure(info, shouldHaveFlag(currentState, TERMINATED_STATE));
    }
    return this;
  }
}
