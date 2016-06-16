/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket;

import io.reactivesocket.internal.Responder;
import io.reactivesocket.lease.NullLeaseGovernor;
import io.reactivesocket.lease.UnlimitedLeaseGovernor;

public interface LeaseGovernor {
    LeaseGovernor NULL_LEASE_GOVERNOR = new NullLeaseGovernor();
    LeaseGovernor UNLIMITED_LEASE_GOVERNOR = new UnlimitedLeaseGovernor();

    /**
     * Register a responder into the LeaseGovernor.
     * This give the responsibility to the leaseGovernor to send lease to the responder.
     *
     * @param responder the responder that will receive lease
     */
    void register(Responder responder);

    /**
     * Unregister a responder from the LeaseGovernor.
     * Depending on the implementation, this action may trigger a rebalancing of
     * the tickets/window to the remaining responders.
     * @param responder the responder to be removed
     */
    void unregister(Responder responder);

    /**
     * Check if the message received by the responder is valid (i.e. received during a
     * valid lease window)
     * This action my have side effect in the LeaseGovernor.
     *
     * @param responder receiving the message
     * @param frame the received frame
     * @return
     */
    boolean accept(Responder responder, Frame frame);
}
