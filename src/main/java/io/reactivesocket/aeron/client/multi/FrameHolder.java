package io.reactivesocket.aeron.client.multi;

import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.Constants;
import org.reactivestreams.Subscription;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

/**
 * Holds a frame and the publication that it's supposed to be sent on.
 * Pools instances on an {@link uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue}
 */
class FrameHolder {
    private static final ThreadLocal<OneToOneConcurrentArrayQueue<FrameHolder>> FRAME_HOLDER_QUEUE
        = ThreadLocal.withInitial(() -> new OneToOneConcurrentArrayQueue<>(Constants.QUEUE_SIZE));

    private Frame frame;
    private Subscription s;

    private FrameHolder() {}

    public static FrameHolder get(Frame frame, Subscription s) {
        FrameHolder frameHolder = FRAME_HOLDER_QUEUE.get().poll();

        if (frameHolder == null) {
            frameHolder = new FrameHolder();
        }

        frameHolder.frame = frame;
        frameHolder.s = s;

        return frameHolder;
    }

    public Frame getFrame() {
        return frame;
    }

    public void release() {
        if (s != null) {
            s.request(1);
        }

        frame.release();
        FRAME_HOLDER_QUEUE.get().offer(this);
    }
}
