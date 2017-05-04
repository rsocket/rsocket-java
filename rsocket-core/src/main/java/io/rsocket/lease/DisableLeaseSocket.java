/*
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

package io.rsocket.lease;

import io.rsocket.ReactiveSocket;
import io.rsocket.util.ReactiveSocketProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link LeaseHonoringSocket} that does not expect to receive any leases and {@link #accept(Lease)} throws an error.
 */
public class DisableLeaseSocket extends ReactiveSocketProxy implements LeaseHonoringSocket {

    private static final Logger logger = LoggerFactory.getLogger(DisableLeaseSocket.class);

    public DisableLeaseSocket(ReactiveSocket source) {
        super(source);
    }

    @Override
    public void accept(Lease lease) {
        logger.info("Leases are disabled but received a lease from the peer. " + lease);
    }
}
