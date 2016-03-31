/**
 * Copyright 2016 Netflix, Inc.
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
package io.reactivesocket.aeron.client;

import io.aeron.Subscription;
import io.reactivesocket.aeron.internal.Loggable;
import rx.functions.Action0;

import java.util.List;

class PollingAction implements Action0, Loggable {
    private final List<ClientAeronManager.SubscriptionGroup> subscriptionGroups;
    private final List<ClientAeronManager.ClientAction> clientActions;

    public PollingAction(
            List<ClientAeronManager.SubscriptionGroup> subscriptionGroups,
            List<ClientAeronManager.ClientAction> clientActions) {
        this.subscriptionGroups = subscriptionGroups;
        this.clientActions = clientActions;
    }

    @Override
    public void call() {
        try {
            for (ClientAeronManager.SubscriptionGroup sg : subscriptionGroups) {
                try {
                    int poll = 0;
                    do {
                        Subscription subscription = sg.getSubscription();
                        if (!subscription.isClosed()) {
                            poll = subscription.poll(sg.getFragmentAssembler(), Integer.MAX_VALUE);
                        }
                    } while (poll > 0);

                    for (ClientAeronManager.ClientAction action : clientActions) {
                        action.call();
                    }
                } catch (Throwable t) {
                    error("error polling aeron subscription", t);
                }
            }

        } catch (Throwable t) {
            error("error in client polling loop", t);
        }
    }
}
