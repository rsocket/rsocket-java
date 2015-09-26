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
package io.reactivesocket.aeron.server;

import io.reactivesocket.aeron.internal.Loggable;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.AvailableImageHandler;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.UnavailableImageHandler;

import java.util.concurrent.CopyOnWriteArrayList;

import static io.reactivesocket.aeron.internal.Constants.SERVER_IDLE_STRATEGY;

/**
 * Class that manages the Aeron instance and the server's polling thread. Lets you register more
 * than one NewImageHandler to Aeron after the it's the Aeron instance has started
 */
public class ServerAeronManager implements Loggable {
    private static final ServerAeronManager INSTANCE = new ServerAeronManager();

    private final Aeron aeron;

    private CopyOnWriteArrayList<AvailableImageHandler> availableImageHandlers = new CopyOnWriteArrayList<>();

    private CopyOnWriteArrayList<UnavailableImageHandler> unavailableImageHandlers = new CopyOnWriteArrayList<>();

    private CopyOnWriteArrayList<FragmentAssemblerHolder> fragmentAssemblerHolders = new CopyOnWriteArrayList<>();

    private class FragmentAssemblerHolder {
        private Subscription subscription;
        private FragmentAssembler fragmentAssembler;

        public FragmentAssemblerHolder(Subscription subscription, FragmentAssembler fragmentAssembler) {
            this.subscription = subscription;
            this.fragmentAssembler = fragmentAssembler;
        }
    }

    public ServerAeronManager() {
        final Aeron.Context ctx = new Aeron.Context();
        ctx.availableImageHandler(this::availableImageHandler);
        ctx.unavailableImageHandler(this::unavailableImage);
        ctx.errorHandler(t -> error("an exception occurred", t));

        aeron = Aeron.connect(ctx);

        poll();
    }

    public static ServerAeronManager getInstance() {
        return INSTANCE;
    }

    public void addAvailableImageHander(AvailableImageHandler handler) {
        availableImageHandlers.add(handler);
    }

    public void addUnavailableImageHandler(UnavailableImageHandler handler) {
        unavailableImageHandlers.add(handler);
    }

    public void addSubscription(Subscription subscription, FragmentAssembler fragmentAssembler) {
        debug("Adding subscription with session id {}", subscription.streamId());
        fragmentAssemblerHolders.add(new FragmentAssemblerHolder(subscription, fragmentAssembler));
    }

    public void removeSubscription(Subscription subscription) {
        debug("Removing subscription with session id {}", subscription.streamId());
        fragmentAssemblerHolders.removeIf(s -> s.subscription == subscription);
    }

    private void availableImageHandler(Image image, Subscription subscription, long joiningPosition, String sourceIdentity) {
        availableImageHandlers
            .forEach(handler -> handler.onAvailableImage(image, subscription, joiningPosition, sourceIdentity));
    }

    private void unavailableImage(Image image, Subscription subscription, long position) {
        unavailableImageHandlers
            .forEach(handler -> handler.onUnavailableImage(image, subscription, position));
    }

    public Aeron getAeron() {
        return aeron;
    }

    void poll() {
        Thread dutyThread = new Thread(() -> {
            for (;;) {
                int poll = 0;
                for (FragmentAssemblerHolder sh : fragmentAssemblerHolders) {
                    try {
                        poll += sh.subscription.poll(sh.fragmentAssembler, Integer.MAX_VALUE);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
                SERVER_IDLE_STRATEGY.idle(poll);
            }
        });
        dutyThread.setName("reactive-socket-aeron-server");
        dutyThread.setDaemon(true);
        dutyThread.start();
    }
}
