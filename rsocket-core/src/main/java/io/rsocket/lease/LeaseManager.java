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

import io.netty.buffer.ByteBuf;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

class LeaseManager {
  private volatile LeaseImpl currentLease = LeaseImpl.empty();
  private final String tag;
  @Nullable private volatile LeaseEventsListener leaseEventsListener;

  public LeaseManager(@Nonnull String tag) {
    this.tag = Objects.requireNonNull(tag, "tag");
  }

  public double availability() {
    LeaseImpl l = currentLease;
    return l.isValid() ? l.getAvailableRequests() / (double) l.getStartingAllowedRequests() : 0.0;
  }

  public void updateLease(int timeToLiveMillis, int numberOfRequests, @Nullable ByteBuf metadata) {
    currentLease = LeaseImpl.create(timeToLiveMillis, numberOfRequests, metadata);
  }

  public Lease getLease() {
    return currentLease;
  }

  /** reserves Lease on requester method invocation */
  public void reserveLease() {
    currentLease.reserve();
  }

  /** reserves and uses Lease on requester subscription */
  public Lease reserveAndUseLease() {
    LeaseImpl l = currentLease;
    boolean success = l.reserveAndUse();
    onLeaseUse(l, success);
    return success ? null : l;
  }

  /**
   * tries to use Lease if not expired and there are allowed requests
   *
   * @return null if current Lease used successfully, current Lease otherwise
   */
  @Nullable
  public Lease useLease() {
    LeaseImpl l = currentLease;
    boolean success = l.use();
    onLeaseUse(l, success);
    return success ? null : l;
  }

  public String getTag() {
    return tag;
  }

  /**
   * sets listener for Lease events
   *
   * @param leaseEventsListener listener for Lease events
   */
  public void leaseEventsListener(LeaseEventsListener leaseEventsListener) {
    this.leaseEventsListener = Objects.requireNonNull(leaseEventsListener, "leaseEventsListener");
  }

  @Override
  public String toString() {
    return "LeaseManager{" + "tag='" + tag + '\'' + '}';
  }

  private void onLeaseUse(LeaseImpl l, boolean success) {
    LeaseEventsListener le = leaseEventsListener;
    if (le != null) {
      LeaseEventsListener.Type eventType =
          success ? LeaseEventsListener.Type.SUCCESS : LeaseEventsListener.Type.REJECT;
      le.onEvent(l, eventType);
    }
  }
}
