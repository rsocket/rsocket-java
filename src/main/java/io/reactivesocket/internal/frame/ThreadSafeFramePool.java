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
package io.reactivesocket.internal.frame;

import io.reactivesocket.Frame;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class ThreadSafeFramePool implements FramePool
{
    private static final int MAX_CACHED_FRAMES = 16;

    private final OneToOneConcurrentArrayQueue<Frame> frameQueue;
    private final OneToOneConcurrentArrayQueue<MutableDirectBuffer> directBufferQueue;

    public ThreadSafeFramePool()
    {
        this(MAX_CACHED_FRAMES, MAX_CACHED_FRAMES);
    }

    public ThreadSafeFramePool(final int frameQueueLength, final int directBufferQueueLength)
    {
        frameQueue = new OneToOneConcurrentArrayQueue<>(frameQueueLength);
        directBufferQueue = new OneToOneConcurrentArrayQueue<>(directBufferQueueLength);
    }

    public Frame acquireFrame(int size)
    {
        final MutableDirectBuffer directBuffer = acquireMutableDirectBuffer(size);

        Frame frame = pollFrame();
        if (null == frame)
        {
            frame = Frame.allocate(directBuffer);
        }

        return frame;
    }

    public Frame acquireFrame(ByteBuffer byteBuffer)
    {
        return Frame.allocate(new UnsafeBuffer(byteBuffer));
    }

    public Frame acquireFrame(MutableDirectBuffer mutableDirectBuffer)
    {
        Frame frame = pollFrame();
        if (null == frame)
        {
            frame = Frame.allocate(mutableDirectBuffer);
        }

        return frame;
    }

    public MutableDirectBuffer acquireMutableDirectBuffer(ByteBuffer byteBuffer)
    {
        MutableDirectBuffer directBuffer = pollMutableDirectBuffer();
        if (null == directBuffer)
        {
            directBuffer = new UnsafeBuffer(byteBuffer);
        }

        return directBuffer;
    }

    public MutableDirectBuffer acquireMutableDirectBuffer(int size)
    {
        UnsafeBuffer directBuffer = (UnsafeBuffer)pollMutableDirectBuffer();
        if (null == directBuffer || directBuffer.capacity() < size)
        {
            directBuffer = new UnsafeBuffer(ByteBuffer.allocate(size));
        }
        else
        {
            directBuffer.byteBuffer().limit(size).position(0);
        }

        return directBuffer;
    }

    public void release(Frame frame)
    {
        synchronized (frameQueue)
        {
            frameQueue.offer(frame);
        }
    }

    public void release(MutableDirectBuffer mutableDirectBuffer)
    {
        synchronized (directBufferQueue)
        {
            directBufferQueue.offer(mutableDirectBuffer);
        }
    }

    private Frame pollFrame()
    {
        synchronized (frameQueue)
        {
            return frameQueue.poll();
        }
    }

    private MutableDirectBuffer pollMutableDirectBuffer()
    {
        synchronized (directBufferQueue)
        {
            return directBufferQueue.poll();
        }
    }
}
