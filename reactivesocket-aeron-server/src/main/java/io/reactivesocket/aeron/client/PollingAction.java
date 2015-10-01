package io.reactivesocket.aeron.client;

import io.reactivesocket.aeron.internal.Loggable;
import rx.functions.Action0;
import uk.co.real_logic.aeron.Subscription;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

class PollingAction implements Action0, Loggable {

    private final ReentrantLock pollLock;
    private final ReentrantLock clientActionLock;
    private final int threadId;
    private final List<ClientAeronManager.SubscriptionGroup> subscriptionGroups;
    private final List<ClientAeronManager.ClientAction> clientActions;

    public PollingAction(
            ReentrantLock pollLock,
            ReentrantLock clientActionLock,
            int threadId,
            List<ClientAeronManager.SubscriptionGroup> subscriptionGroups,
            List<ClientAeronManager.ClientAction> clientActions) {
        this.pollLock = pollLock;
        this.clientActionLock = clientActionLock;
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

                    //if (clientActionLock.tryLock()) {
                        for (ClientAeronManager.ClientAction action : clientActions) {
                            action.call(threadId);
                        }
                    //}
                } catch (Throwable t) {
                    error("error polling aeron subscription on thread with id " + threadId, t);
                } finally {
                    if (pollLock.isHeldByCurrentThread()) {
                        pollLock.unlock();
                    }

                    if (clientActionLock.isHeldByCurrentThread()) {
                        clientActionLock.unlock();
                    }
                }
            }

        } catch (Throwable t) {
            error("error in client polling loop on thread with id " + threadId, t);
        }
    }
}
