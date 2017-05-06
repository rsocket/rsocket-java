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

import io.rsocket.RSocket;
import io.rsocket.exceptions.RejectedException;
import io.rsocket.lease.DefaultLeaseHonoringSocketTest.SocketHolder;
import io.rsocket.test.util.MockRSocket;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.LongSupplier;

@RunWith(Parameterized.class)
public class DefaultLeaseHonoringSocketTest extends DefaultLeaseTest<SocketHolder> {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { 0, 0, RejectedException.class, 0, null },
                { 1, 10, UnsupportedOperationException.class, 1, null },
                { 1, 10, RejectedException.class, 0, new LongSupplier() {
                    @Override
                    public long getAsLong() {
                        return Long.MAX_VALUE;
                    }
                }},
        });
    }

    @Override
    protected SocketHolder init() {
        return SocketHolder.newHolder(currentTimeSupplier).sendLease(permits, ttl);
    }

    @Override
    protected RSocket getRSocket(SocketHolder state) {
        return state.getRSocket();
    }

    @Override
    protected MockRSocket getMockSocket(SocketHolder state) {
        return state.getMockSocket();
    }

    public static class SocketHolder {

        private final MockRSocket mockSocket;
        private final DefaultLeaseHonoringSocket reactiveSocket;

        public SocketHolder(MockRSocket mockSocket, DefaultLeaseHonoringSocket reactiveSocket) {
            this.mockSocket = mockSocket;
            this.reactiveSocket = reactiveSocket;
        }

        public MockRSocket getMockSocket() {
            return mockSocket;
        }

        public DefaultLeaseHonoringSocket getRSocket() {
            return reactiveSocket;
        }

        public static SocketHolder newHolder(LongSupplier currentTimeSupplier) {
            LongSupplier _currentTimeSupplier = null == currentTimeSupplier? () -> -1 : currentTimeSupplier;
            AbstractSocketRule<DefaultLeaseHonoringSocket> rule = new AbstractSocketRule<DefaultLeaseHonoringSocket>() {
                @Override
                protected DefaultLeaseHonoringSocket newSocket(RSocket delegate) {
                    return new DefaultLeaseHonoringSocket(delegate, _currentTimeSupplier);
                }
            };
            rule.init();
            return new SocketHolder(rule.getMockSocket(), rule.getRSocket());
        }

        public SocketHolder sendLease(int permits, int ttl) {
            reactiveSocket.accept(new LeaseImpl(permits, ttl));
            return this;
        }
    }
}