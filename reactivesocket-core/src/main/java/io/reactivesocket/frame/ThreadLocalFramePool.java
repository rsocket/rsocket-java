/*
 * Copyright 2016 Netflix, Inc.
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
package io.reactivesocket.frame;

import io.reactivesocket.Frame;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class ThreadLocalFramePool implements FramePool {
    private static final int MAX_CAHED_FRAMES_PER_THREAD = 16;

    private static final ThreadLocal<OneToOneConcurrentArrayQueue<Frame>> PER_THREAD_FRAME_QUEUE =
        ThreadLocal.withInitial(() -> new OneToOneConcurrentArrayQueue<>(MAX_CAHED_FRAMES_PER_THREAD));

    private static final ThreadLocal<OneToOneConcurrentArrayQueue<MutableDirectBuffer>> PER_THREAD_DIRECTBUFFER_QUEUE =
        ThreadLocal.withInitial(() -> new OneToOneConcurrentArrayQueue<>(MAX_CAHED_FRAMES_PER_THREAD));

    public Frame acquireFrame(int size) {
        final MutableDirectBuffer directBuffer = acquireMutableDirectBuffer(size);

        Frame frame = pollFrame();
        if (null == frame) {
            frame = Frame.allocate(directBuffer);
        }

        return frame;
    }

    public Frame acquireFrame(ByteBuffer byteBuffer) {
        return Frame.allocate(new UnsafeBuffer(byteBuffer));
    }

    public void release(Frame frame)
    {
        PER_THREAD_FRAME_QUEUE.get().offer(frame);
    }

    public Frame acquireFrame(MutableDirectBuffer mutableDirectBuffer) {
        Frame frame = pollFrame();
        if (null == frame) {
            frame = Frame.allocate(mutableDirectBuffer);
        }

        return frame;
    }

    public MutableDirectBuffer acquireMutableDirectBuffer(ByteBuffer byteBuffer) {
        MutableDirectBuffer directBuffer = pollMutableDirectBuffer();
        if (null == directBuffer) {
            directBuffer = new UnsafeBuffer(byteBuffer);
        }

        return directBuffer;
    }

    public MutableDirectBuffer acquireMutableDirectBuffer(int size) {
        UnsafeBuffer directBuffer = (UnsafeBuffer)pollMutableDirectBuffer();
        if (null == directBuffer || directBuffer.byteBuffer().capacity() < size) {
            directBuffer = new UnsafeBuffer(ByteBuffer.allocate(size));
        } else {
            directBuffer.byteBuffer().limit(size).position(0);
        }

        return directBuffer;
    }

    public void release(MutableDirectBuffer mutableDirectBuffer) {
        PER_THREAD_DIRECTBUFFER_QUEUE.get().offer(mutableDirectBuffer);
    }

    private Frame pollFrame()
    {
        return PER_THREAD_FRAME_QUEUE.get().poll();
    }

    private MutableDirectBuffer pollMutableDirectBuffer() {
        return PER_THREAD_DIRECTBUFFER_QUEUE.get().poll();
    }
}
