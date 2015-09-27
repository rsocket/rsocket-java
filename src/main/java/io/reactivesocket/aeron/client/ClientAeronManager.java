/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.aeron.client;

import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.Loggable;
import rx.Scheduler;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class for managing the Aeron on the client side.
 */
public class ClientAeronManager implements Loggable {
    private static final ClientAeronManager INSTANCE = new ClientAeronManager();

    private final CopyOnWriteArrayList<SubscriptionGroup> subscriptionGroups;

    private final Aeron aeron;

    private final Scheduler.Worker[] workers = new Scheduler.Worker[Constants.CONCURRENCY];

    private ClientAeronManager() {
        this.subscriptionGroups = new CopyOnWriteArrayList<>();

        final Aeron.Context ctx = new Aeron.Context();
        ctx.errorHandler(t -> error("an exception occurred", t));

        aeron = Aeron.connect(ctx);

        poll();
    }

    public static ClientAeronManager getInstance() {
        return INSTANCE;
    }

    /**
     * Finds a SubscriptionGroup from the sessionId of one the servers in the group
     *
     * @param sessionId the session id whose SubscriptionGroup you want to find
     * @return an Optional of SubscriptionGroup
     */
    public Optional<SubscriptionGroup> find(final int sessionId) {
        return subscriptionGroups
            .stream()
            .filter(sg -> {
                boolean found = false;

                for (Subscription subscription : sg.subscriptions) {
                    Image image = subscription.getImage(sessionId);
                    if (image != null) {
                        found = true;
                        break;
                    }
                }

                return found;
            })
            .findFirst();
    }

    public void removeClientAction(int id) {
        subscriptionGroups
            .forEach(sg ->
                sg
                    .getClientActions()
                    .removeIf(c -> {
                        if (c.id == id) {
                            debug("removing client action for id => {}", id);
                            try {
                                c.close();
                            } catch (Throwable e) {
                                debug("an exception occurred trying to close connection {}", e, id);
                            }
                            return true;
                        } else {
                            return false;
                        }
                    })
            );
    }


    public boolean hasSubscriptionForChannel(String subscriptionChannel) {
        return subscriptionGroups
            .stream()
            .anyMatch(sg -> sg.getChannel().equals(subscriptionChannel));
    }

    public Aeron getAeron() {
        return aeron;
    }

    /**
     * Adds an Aeron subscription to be polled. This method will create a subscription for each of the polling threads.
     *
     * @param subscriptionChannel the channel to create subscriptions on
     * @param streamId the stream id to create subscriptions on
     * @param fragmentHandlerFactory factory that creates a fragment handler that is aware of the thread that is call it.
     */
    public void addSubscription(String subscriptionChannel, int streamId, Func1<Integer, ThreadIdAwareFragmentHandler> fragmentHandlerFactory) {
        if (!hasSubscriptionForChannel(subscriptionChannel)) {


            debug("Creating a subscriptions to channel => {}", subscriptionChannel);
            Subscription[] subscriptions = new Subscription[Constants.CONCURRENCY];
            for (int i = 0; i < Constants.CONCURRENCY; i++) {
                subscriptions[i] = aeron.addSubscription(subscriptionChannel, streamId);
                debug("Subscription created with session id  => {} and threadId => {}", subscriptions[i].streamId(), i);
            }
            SubscriptionGroup subscriptionGroup = new SubscriptionGroup(subscriptionChannel, subscriptions, fragmentHandlerFactory);
            subscriptionGroups.add(subscriptionGroup);
            debug("Subscriptions created to channel => {}", subscriptionChannel);

        } else {
            debug("Subscription already exists for channel => {}", subscriptionChannel);
        }
    }

    /*
     * Starts polling for the Aeron client. Will run register client actions and will automatically start polling
     * subscriptions
     */
    private void poll() {
        info("ReactiveSocket Aeron Client concurreny is {}", Constants.CONCURRENCY);
        final ReentrantLock pollLock = new ReentrantLock();
        final ReentrantLock clientActionLock = new ReentrantLock();
        for (int i = 0; i < Constants.CONCURRENCY; i++) {
            final int threadId = i;
            workers[threadId] = Schedulers.computation().createWorker();
            workers[threadId].schedulePeriodically(() -> {
                try {
                    for (SubscriptionGroup sg : subscriptionGroups) {
                        try {
                            if (pollLock.tryLock()) {
                                Subscription subscription = sg.getSubscriptions()[threadId];
                                subscription.poll(sg.getFragmentAssembler(threadId), Integer.MAX_VALUE);
                            }

                            if (clientActionLock.tryLock()) {
                                final List<ClientAction> clientActions = sg.getClientActions();

                                for (ClientAction a : clientActions) {
                                    a.call(threadId);
                                }
                            }
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
            }, 0, 20, TimeUnit.MICROSECONDS);
        }
    }

    /*
     * Inner Classes
     */

    /**
     * Creates a logic group of {@link uk.co.real_logic.aeron.Subscription}s to a particular channel.
     */
    public static class SubscriptionGroup {

        private final static ThreadLocal<FragmentAssembler> threadLocalFragmentAssembler = new ThreadLocal<>();
        private final String channel;
        private final Subscription[] subscriptions;
        private final Func1<Integer, ThreadIdAwareFragmentHandler> fragmentHandlerFactory;

        private final CopyOnWriteArrayList<ClientAction> clientActions;

        public SubscriptionGroup(String channel, Subscription[] subscriptions, Func1<Integer, ThreadIdAwareFragmentHandler> fragmentHandlerFactory) {
            this.channel = channel;
            this.subscriptions = subscriptions;
            this.fragmentHandlerFactory = fragmentHandlerFactory;
            this.clientActions = new CopyOnWriteArrayList<>();
        }

        public String getChannel() {
            return channel;
        }

        public Subscription[] getSubscriptions() {
            return subscriptions;
        }

        public FragmentAssembler getFragmentAssembler(int threadId) {
            FragmentAssembler assembler = threadLocalFragmentAssembler.get();

            if (assembler == null) {
                assembler = new FragmentAssembler(fragmentHandlerFactory.call(threadId));
                threadLocalFragmentAssembler.set(assembler);
            }

            return assembler;
        }

        public List<ClientAction> getClientActions() {
            return clientActions;
        }
    }

    public static abstract class ClientAction implements AutoCloseable {
        protected int id;

        public ClientAction(int id) {
            this.id = id;
        }

        abstract void call(int threadId);

        @Override
        public final boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ClientAction that = (ClientAction) o;

            if (id != that.id) return false;

            return true;
        }

        @Override
        public final int hashCode() {
            return id;
        }
    }

    /**
     * FragmentHandler that is aware of the thread that it is running on. This is useful if you only want a one thread
     * to process a particular message.
     */
    public static abstract class ThreadIdAwareFragmentHandler implements FragmentHandler {
        private int threadId;

        public ThreadIdAwareFragmentHandler(int threadId) {
            this.threadId = threadId;
        }

        public final int getThreadId() {
            return this.threadId;
        }
    }
}
