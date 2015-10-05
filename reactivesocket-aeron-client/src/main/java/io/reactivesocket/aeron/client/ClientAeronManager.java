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

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Class for managing the Aeron on the client side.
 */
public class ClientAeronManager implements Loggable {
    private static final ClientAeronManager INSTANCE = new ClientAeronManager();

    private final CopyOnWriteArrayList<ClientAction> clientActions;

    private final CopyOnWriteArrayList<SubscriptionGroup> subscriptionGroups;

    private final Aeron aeron;

    private final Scheduler.Worker[] workers = new Scheduler.Worker[Constants.CONCURRENCY];

    private ClientAeronManager() {
        this.clientActions = new CopyOnWriteArrayList<>();
        this.subscriptionGroups = new CopyOnWriteArrayList<>();

        final Aeron.Context ctx = new Aeron.Context();
        ctx.errorHandler(t -> error("an exception occurred", t));
        ctx.availableImageHandler((Image image, Subscription subscription, long joiningPosition, String sourceIdentity) ->
            debug("New image available with session id => {} and sourceIdentity  => {} and subscription => {}", image.sessionId(), sourceIdentity, subscription.toString())
        );

        aeron = Aeron.connect(ctx);

        poll();
    }

    public static ClientAeronManager getInstance() {
        return INSTANCE;
    }

    /**
     * Adds a ClientAction on the a list that is run by the polling loop.
     *
     * @param clientAction the {@link io.reactivesocket.aeron.client.ClientAeronManager.ClientAction} to add
     */
    public void addClientAction(ClientAction clientAction) {
        clientActions.add(clientAction);
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
                debug("Subscription created for threadId => {} and channel => {} ", i, subscriptionChannel);
            }
            SubscriptionGroup subscriptionGroup = new SubscriptionGroup(subscriptionChannel, subscriptions, fragmentHandlerFactory);
            subscriptionGroups.add(subscriptionGroup);
            debug("Subscriptions created to channel => {}", subscriptionChannel);

        } else {
            debug("Subscription already exists for channel => {}", subscriptionChannel);
        }
    }

    /*
     * Starts polling for the Aeron client. Will run registered client actions and will automatically start polling
     * subscriptions
     */
    void poll() {
        info("ReactiveSocket Aeron Client concurreny is {}", Constants.CONCURRENCY);
        for (int i = 0; i < Constants.CONCURRENCY; i++) {
            final int threadId = i;
            workers[threadId] = Schedulers.computation().createWorker();
            workers[threadId].schedulePeriodically(new
                    PollingAction(threadId, subscriptionGroups, clientActions),
                0, 20, TimeUnit.MICROSECONDS);
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

        public SubscriptionGroup(String channel, Subscription[] subscriptions, Func1<Integer, ThreadIdAwareFragmentHandler> fragmentHandlerFactory) {
            this.channel = channel;
            this.subscriptions = subscriptions;
            this.fragmentHandlerFactory = fragmentHandlerFactory;
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
    }

    @FunctionalInterface
    public interface ClientAction {
        void call(int threadId);
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
