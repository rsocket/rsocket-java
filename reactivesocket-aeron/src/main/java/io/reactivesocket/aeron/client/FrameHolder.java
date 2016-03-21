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

import io.reactivesocket.Frame;
import org.HdrHistogram.Recorder;
import org.reactivestreams.Subscription;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

/**
 * Holds a frame and the publication that it's supposed to be sent on.
 * Pools instances on an {@link uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue}
 */
public class FrameHolder {
    private static final ThreadLocal<OneToOneConcurrentArrayQueue<FrameHolder>> FRAME_HOLDER_QUEUE
        = ThreadLocal.withInitial(() -> new OneToOneConcurrentArrayQueue<>(16));

    public static final Recorder histogram = new Recorder(3600000000000L, 3);

    private Frame frame;
    private Publication publication;
    private Subscription s;
    private long getTime;

    private FrameHolder() {}

    public static FrameHolder get(Frame frame, Publication publication, Subscription s) {
        FrameHolder frameHolder = FRAME_HOLDER_QUEUE.get().poll();

        if (frameHolder == null) {
            frameHolder = new FrameHolder();
        }

        frameHolder.frame = frame;
        frameHolder.s = s;
        frameHolder.publication = publication;
        frameHolder.getTime = System.nanoTime();

        return frameHolder;
    }

    public Frame getFrame() {
        return frame;
    }

    public Publication getPublication() {
        return publication;
    }

    public void release() {
        if (s != null) {
            s.request(1);
        }

        frame.release();
        FRAME_HOLDER_QUEUE.get().offer(this);

        histogram.recordValue(System.nanoTime() - getTime);
    }
}
