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

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.Loggable;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Class for managing the Aeron on the client side.
 */
public class ClientAeronManager implements Loggable {
    private static final ClientAeronManager INSTANCE = new ClientAeronManager();

    /**
     * Enables running the client with an embedded Aeron {@link MediaDriver} so you don't have to run
     * the driver in a separate process. To enable this option you need to set the reactivesocket.aeron.clientEmbeddedDriver
     * to true
     */
    static {
       if (Constants.CLIENT_EMBEDDED_AERON_DRIVER) {
           System.out.println("+++ Launching embedded media driver");
           final MediaDriver.Context context = new MediaDriver.Context();
           context.dirsDeleteOnStart(true);
           context.threadingMode(ThreadingMode.SHARED_NETWORK);
           context.conductorIdleStrategy(new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(10)));
           context.senderIdleStrategy(new BackoffIdleStrategy(5, 10, 100, 1000));
           context.receiverIdleStrategy(new BackoffIdleStrategy(5, 10, 100, 1000));
           MediaDriver.launch(context);
       }
    }

    private final CopyOnWriteArrayList<ClientAction> clientActions;

    private final CopyOnWriteArrayList<SubscriptionGroup> subscriptionGroups;

    private final Aeron aeron;

    private final Scheduler.Worker worker;

    private ClientAeronManager() {
        this.clientActions = new CopyOnWriteArrayList<>();
        this.subscriptionGroups = new CopyOnWriteArrayList<>();

        final Aeron.Context ctx = new Aeron.Context();
        ctx.errorHandler(t -> error("an exception occurred", t));
        ctx.availableImageHandler((Image image) ->
            debug("New image available with session id => {} and sourceIdentity  => {} and subscription => {}", image.sessionId(), image.sourceIdentity(), image.subscription().toString())
        );

        aeron = Aeron.connect(ctx);
        worker = Schedulers.computation().createWorker();
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
     * @param fragmentHandler fragment handler that is aware of the thread that is call it.
     */
    public void addSubscription(String subscriptionChannel, int streamId, FragmentHandler fragmentHandler) {
        if (!hasSubscriptionForChannel(subscriptionChannel)) {

            debug("Creating a subscriptions to channel => {}", subscriptionChannel);
            Subscription subscription = aeron.addSubscription(subscriptionChannel, streamId);
            debug("Subscription created channel => {} ", subscriptionChannel);
            SubscriptionGroup subscriptionGroup = new SubscriptionGroup(subscriptionChannel, subscription, fragmentHandler);
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
        info("ReactiveSocket Aeron Client poll");
        worker.schedulePeriodically(new PollingAction(subscriptionGroups, clientActions),
            0, 20, TimeUnit.MICROSECONDS);
    }

    /*
     * Inner Classes
     */

    /**
     * Creates a logic group of {@link io.aeron.Subscription}s to a particular channel.
     */
    public static class SubscriptionGroup {

        private final static ThreadLocal<FragmentAssembler> threadLocalFragmentAssembler = new ThreadLocal<>();
        private final String channel;
        private final Subscription subscription;
        private final FragmentHandler fragmentHandler;

        public SubscriptionGroup(String channel, Subscription subscription, FragmentHandler fragmentHandler) {
            this.channel = channel;
            this.subscription = subscription;
            this.fragmentHandler = fragmentHandler;
        }

        public String getChannel() {
            return channel;
        }

        public Subscription getSubscription() {
            return subscription;
        }

        public FragmentAssembler getFragmentAssembler() {
            FragmentAssembler assembler = threadLocalFragmentAssembler.get();

            if (assembler == null) {
                assembler = new FragmentAssembler(fragmentHandler);
                threadLocalFragmentAssembler.set(assembler);
            }

            return assembler;
        }
    }

    @FunctionalInterface
    public interface ClientAction {
        void call();
    }
}
