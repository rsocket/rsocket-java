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

package io.reactivesocket.reactivestreams.extensions.internal;

import org.junit.Test;
import org.reactivestreams.Subscription;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class SerializedSubscriptionTest {

    @Test
    public void testReplace() throws Exception {
        MockSubscription first = new MockSubscription();
        SerializedSubscription subscription = new SerializedSubscription(first);
        subscription.request(10);
        assertThat("Unexpected requested count.", first.getRequested(), is(10));
        subscription.onItemReceived();
        MockSubscription second = new MockSubscription();
        subscription.replaceSubscription(second);
        assertThat("Previous subscription not cancelled.", first.isCancelled(), is(true));
        assertThat("Unexpected requested count.", second.getRequested(), is(9));
        subscription.request(10);
        assertThat("Unexpected requested count.", second.getRequested(), is(19));
    }

    @Test
    public void testReplacePostCancel() throws Exception {
        MockSubscription first = new MockSubscription();
        SerializedSubscription subscription = new SerializedSubscription(first);
        assertThat("Unexpected cancelled state.", first.isCancelled(), is(false));
        subscription.cancel();
        assertThat("Unexpected cancelled state.", first.isCancelled(), is(true));
        MockSubscription second = new MockSubscription();
        subscription.replaceSubscription(second);
        assertThat("Subscription not cancelled.", second.isCancelled(), is(true));
    }

    private static class MockSubscription implements Subscription {
        private int requested;
        private volatile boolean cancelled;

        @Override
        public synchronized void request(long n) {
            requested = FlowControlHelper.incrementRequestN(requested, n);
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        public int getRequested() {
            return requested;
        }

        public boolean isCancelled() {
            return cancelled;
        }
    }
}