package io.reactivesocket.aeron.server;

import io.reactivesocket.aeron.internal.Loggable;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.AvailableImageHandler;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Subscription;

import java.util.concurrent.CopyOnWriteArrayList;

import static io.reactivesocket.aeron.internal.Constants.SERVER_IDLE_STRATEGY;

/**
 * Class that manages the Aeron instance and the server's polling thread. Lets you register more
 * than one NewImageHandler to Aeron after the it's the Aeron instance has started
 */
public class ServerAeronManager implements Loggable {
    private static final ServerAeronManager INSTANCE = new ServerAeronManager();

    private final Aeron aeron;

    private CopyOnWriteArrayList<AvailableImageHandler> imageHandlers = new CopyOnWriteArrayList<>();

    private CopyOnWriteArrayList<FragmentAssemblerHolder> subscriptions = new CopyOnWriteArrayList<>();

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
        ctx.errorHandler(t -> error("an exception occurred", t));

        aeron = Aeron.connect(ctx);

        poll();
    }

    public static ServerAeronManager getInstance() {
        return INSTANCE;
    }

    public void addAvailableImageHander(AvailableImageHandler handler) {
        imageHandlers.add(handler);
    }

    public void addSubscription(Subscription subscription, FragmentAssembler fragmentAssembler) {
        subscriptions.add(new FragmentAssemblerHolder(subscription, fragmentAssembler));
    }

    public void removeSubscription(Subscription subscription) {
        subscriptions.removeIf(s -> s.subscription == subscription);
    }

    private void availableImageHandler(Image image, Subscription subscription, long joiningPosition, String sourceIdentity) {
        imageHandlers
            .forEach(handler -> handler.onAvailableImage(image, subscription, joiningPosition, sourceIdentity));
    }

    public Aeron getAeron() {
        return aeron;
    }

    void poll() {
        Thread dutyThread = new Thread(() -> {
            for (;;) {
                int poll = 0;
                for (FragmentAssemblerHolder sh : subscriptions) {
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
