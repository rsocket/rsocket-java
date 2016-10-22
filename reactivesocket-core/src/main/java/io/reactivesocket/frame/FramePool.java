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

import java.nio.ByteBuffer;

public interface FramePool
{
    Frame acquireFrame(final int size);

    Frame acquireFrame(final ByteBuffer byteBuffer);

    Frame acquireFrame(final MutableDirectBuffer mutableDirectBuffer);

    MutableDirectBuffer acquireMutableDirectBuffer(final int size);

    MutableDirectBuffer acquireMutableDirectBuffer(final ByteBuffer byteBuffer);

    void release(final Frame frame);

    void release(final MutableDirectBuffer mutableDirectBuffer);
}
