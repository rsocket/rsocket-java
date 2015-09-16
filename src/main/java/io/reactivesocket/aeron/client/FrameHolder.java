package io.reactivesocket.aeron.client;

import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.Constants;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;

/**
 * Holds a frame and the publication that it's supposed to be sent on.
 * Pools instances on an {@link uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue}
 */
class FrameHolder {
    private static ManyToOneConcurrentArrayQueue<FrameHolder> FRAME_HOLDER_QUEUE
        = new ManyToOneConcurrentArrayQueue<>(Constants.QUEUE_SIZE);

    static {
        for (int i = 0; i < Constants.QUEUE_SIZE; i++) {
            FRAME_HOLDER_QUEUE.offer(new FrameHolder());
        }
    }

    private Publication publication;
    private Frame frame;

    private FrameHolder() {}

    public static FrameHolder get(Frame frame, Publication publication) {
        FrameHolder frameHolder = FRAME_HOLDER_QUEUE.poll();

        if (frameHolder == null) {
            frameHolder = new FrameHolder();
        }

        frameHolder.frame = frame;
        frameHolder.publication = publication;

        return frameHolder;
    }

    public Publication getPublication() {
        return publication;
    }

    public Frame getFrame() {
        return frame;
    }

    public void release() {
        frame.release();
        FRAME_HOLDER_QUEUE.offer(this);
    }
}
