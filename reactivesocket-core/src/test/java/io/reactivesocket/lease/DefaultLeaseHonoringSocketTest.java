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

package io.reactivesocket.lease;

import io.reactivesocket.Frame;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.exceptions.RejectedException;
import io.reactivesocket.lease.DefaultLeaseHonoringSocketTest.SocketHolder;
import io.reactivesocket.test.util.MockReactiveSocket;
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
    protected ReactiveSocket getReactiveSocket(SocketHolder state) {
        return state.getReactiveSocket();
    }

    @Override
    protected MockReactiveSocket getMockSocket(SocketHolder state) {
        return state.getMockSocket();
    }

    public static class SocketHolder {

        private final MockReactiveSocket mockSocket;
        private final DefaultLeaseHonoringSocket reactiveSocket;

        public SocketHolder(MockReactiveSocket mockSocket, DefaultLeaseHonoringSocket reactiveSocket) {
            this.mockSocket = mockSocket;
            this.reactiveSocket = reactiveSocket;
        }

        public MockReactiveSocket getMockSocket() {
            return mockSocket;
        }

        public DefaultLeaseHonoringSocket getReactiveSocket() {
            return reactiveSocket;
        }

        public static SocketHolder newHolder(LongSupplier currentTimeSupplier) {
            LongSupplier _currentTimeSupplier = null == currentTimeSupplier? () -> -1 : currentTimeSupplier;
            AbstractSocketRule<DefaultLeaseHonoringSocket> rule = new AbstractSocketRule<DefaultLeaseHonoringSocket>() {
                @Override
                protected DefaultLeaseHonoringSocket newSocket(ReactiveSocket delegate) {
                    return new DefaultLeaseHonoringSocket(delegate, _currentTimeSupplier);
                }
            };
            rule.init();
            return new SocketHolder(rule.getMockSocket(), rule.getReactiveSocket());
        }

        public SocketHolder sendLease(int permits, int ttl) {
            reactiveSocket.accept(new LeaseImpl(permits, ttl, Frame.NULL_BYTEBUFFER));
            return this;
        }
    }
}