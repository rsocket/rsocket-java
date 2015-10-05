package io.reactivesocket.aeron.client;

import io.reactivesocket.aeron.internal.Loggable;
import rx.functions.Action0;
import uk.co.real_logic.aeron.Subscription;

import java.util.List;

class PollingAction implements Action0, Loggable {
    private final int threadId;
    private final List<ClientAeronManager.SubscriptionGroup> subscriptionGroups;
    private final List<ClientAeronManager.ClientAction> clientActions;

    public PollingAction(
            int threadId,
            List<ClientAeronManager.SubscriptionGroup> subscriptionGroups,
            List<ClientAeronManager.ClientAction> clientActions) {
        this.threadId = threadId;
        this.subscriptionGroups = subscriptionGroups;
        this.clientActions = clientActions;
    }

    @Override
    public void call() {
        try {
            for (ClientAeronManager.SubscriptionGroup sg : subscriptionGroups) {
                try {
                    int poll;
                    do {
                        Subscription subscription = sg.getSubscriptions()[threadId];
                        poll = subscription.poll(sg.getFragmentAssembler(threadId), Integer.MAX_VALUE);
                    } while (poll > 0);

                    for (ClientAeronManager.ClientAction action : clientActions) {
                        action.call(threadId);
                    }
                } catch (Throwable t) {
                    error("error polling aeron subscription on thread with id " + threadId, t);
                }
            }

        } catch (Throwable t) {
            error("error in client polling loop on thread with id " + threadId, t);
        }
    }
}
