package io.reactivesocket.aeron.client;

import io.reactivesocket.aeron.internal.Loggable;
import rx.functions.Action0;
import uk.co.real_logic.aeron.Subscription;

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
