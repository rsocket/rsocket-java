package io.reactivesocket.spectator;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Plugins;
import io.reactivesocket.internal.ClientServerInputMultiplexer;

import java.util.function.Consumer;
import java.util.function.IntSupplier;

/**
 * An implementation of {@link Plugins.FrameCounter} that uses Spectator
 */
public class SpectatorFrameCounter implements Plugins.FrameCounter {
    private final Registry registry;

    private final IntSupplier sampleRate;

    public SpectatorFrameCounter(Registry registry, IntSupplier sampleRate) {
        this.registry = registry;
        this.sampleRate = sampleRate;
    }

    @Override
    public Consumer<Frame> apply(ClientServerInputMultiplexer.Type type) {
        return frame -> counter(frame, type);
    }

    private Consumer<Frame> counter(Frame frame, ClientServerInputMultiplexer.Type type) {
        Counter cancelCounter = registry.counter(FrameType.CANCEL.name(), type.name());
        Counter requestChannelCounter = registry.counter(FrameType.REQUEST_CHANNEL.name(), type.name());
        Counter completeCounter = registry.counter(FrameType.COMPLETE.name(), type.name());
        Counter errorCounter = registry.counter(FrameType.ERROR.name(), type.name());
        Counter extCounter = registry.counter(FrameType.EXT.name(), type.name());
        Counter fireAndForgetCounter = registry.counter(FrameType.FIRE_AND_FORGET.name(), type.name());
        Counter keepAliveCounter = registry.counter(FrameType.KEEPALIVE.name(), type.name());
        Counter leaseCounter = registry.counter(FrameType.LEASE.name(), type.name());
        Counter metadataPushCounter = registry.counter(FrameType.METADATA_PUSH.name(), type.name());
        Counter nextCounter = registry.counter(FrameType.NEXT.name(), type.name());
        Counter nextCompleteCounter = registry.counter(FrameType.NEXT_COMPLETE.name(), type.name());
        Counter payloadCounter = registry.counter(FrameType.PAYLOAD.name(), type.name());
        Counter requestNCounter = registry.counter(FrameType.REQUEST_N.name(), type.name());
        Counter requestResponseCounter = registry.counter(FrameType.REQUEST_RESPONSE.name(), type.name());
        Counter requestStreamCounter = registry.counter(FrameType.REQUEST_STREAM.name(), type.name());
        Counter resumeCounter = registry.counter(FrameType.RESUME.name(), type.name());
        Counter resumeOkCounter = registry.counter(FrameType.RESUME_OK.name(), type.name());
        Counter setupCounter = registry.counter(FrameType.SETUP.name(), type.name());
        Counter undefinedCounter = registry.counter(FrameType.UNDEFINED.name(), type.name());

        return f -> {
            switch (frame.getType()) {
                case CANCEL:
                    cancelCounter.increment();
                    break;
                case REQUEST_CHANNEL:
                    requestChannelCounter.increment();
                    break;
                case COMPLETE:
                    completeCounter.increment();
                    break;
                case ERROR:
                    errorCounter.increment();
                    break;
                case EXT:
                    extCounter.increment();
                    break;
                case FIRE_AND_FORGET:
                    fireAndForgetCounter.increment();
                    break;
                case KEEPALIVE:
                    keepAliveCounter.increment();
                    break;
                case LEASE:
                    leaseCounter.increment();
                    break;
                case METADATA_PUSH:
                    metadataPushCounter.increment();
                    break;
                case NEXT:
                    nextCounter.increment();
                    break;
                case NEXT_COMPLETE:
                    nextCompleteCounter.increment();
                    break;
                case PAYLOAD:
                    payloadCounter.increment();
                    break;
                case REQUEST_N:
                    requestNCounter.increment();
                    break;
                case REQUEST_RESPONSE:
                    requestResponseCounter.increment();
                    break;
                case REQUEST_STREAM:
                    requestStreamCounter.increment();
                    break;
                case RESUME:
                    resumeCounter.increment();
                    break;
                case RESUME_OK:
                    resumeOkCounter.increment();
                    break;
                case SETUP:
                    setupCounter.increment();
                    break;
                case UNDEFINED:
                    undefinedCounter.increment();
                    break;
            }
        };
    }
}
