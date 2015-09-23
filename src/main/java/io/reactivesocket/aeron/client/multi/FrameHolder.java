package io.reactivesocket.aeron.client.multi;

import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.concurrent.ManyToManyConcurrentArrayQueue;
import rx.Subscriber;
import uk.co.real_logic.aeron.Publication;

/**
 * Holds a frame and the publication that it's supposed to be sent on.
 * Pools instances on an {@link ManyToManyConcurrentArrayQueue}
 */
class FrameHolder {
    private static ManyToManyConcurrentArrayQueue<FrameHolder> FRAME_HOLDER_QUEUE
        = new ManyToManyConcurrentArrayQueue<>(Constants.QUEUE_SIZE);

    private Publication publication;
    private Frame frame;
    private Subscriber s;

    private FrameHolder() {}

    public static FrameHolder get(Frame frame, Publication publication, Subscriber s) {
        FrameHolder frameHolder = FRAME_HOLDER_QUEUE.poll();

        if (frameHolder == null) {
            frameHolder = new FrameHolder();
        }

        frameHolder.frame = frame;
        frameHolder.publication = publication;
        frameHolder.s = s;

        return frameHolder;
    }

    public Publication getPublication() {
        return publication;
    }

    public Frame getFrame() {
        return frame;
    }

    public void release() {
        if (s != null) {
            s.onCompleted();
        }

        frame.release();
        FRAME_HOLDER_QUEUE.offer(this);
    }
}
