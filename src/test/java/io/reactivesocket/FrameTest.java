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
package io.reactivesocket;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

public class FrameTest
{
    @Test
    public void testWriteThenRead() {
    	final ByteBuffer helloBuffer = TestUtil.byteBufferFromUtf8String("hello");
        
        Frame f = Frame.from(1, FrameType.REQUEST_RESPONSE, helloBuffer);
        assertEquals("hello", TestUtil.byteToString(f.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, f.getType());
        assertEquals(1, f.getStreamId());

        ByteBuffer b = f.getByteBuffer();

        Frame f2 = Frame.from(b);
        assertEquals("hello", TestUtil.byteToString(f2.getData()));
        assertEquals(FrameType.REQUEST_RESPONSE, f2.getType());
        assertEquals(1, f2.getStreamId());
    }

    @Test
    public void testWrapMessage() {
    	final ByteBuffer helloBuffer = TestUtil.byteBufferFromUtf8String("hello");
        
        Frame f = Frame.from(1, FrameType.REQUEST_RESPONSE, helloBuffer);

        f.wrap(2, FrameType.COMPLETE, "done");
        assertEquals("done", TestUtil.byteToString(f.getData()));
        assertEquals(FrameType.NEXT_COMPLETE, f.getType());
        assertEquals(2, f.getStreamId());
    }

    @Test
    public void testWrapBytes() {
    	final ByteBuffer helloBuffer = TestUtil.byteBufferFromUtf8String("hello");
        final ByteBuffer anotherBuffer = TestUtil.byteBufferFromUtf8String("another");
        
        Frame f = Frame.from(1, FrameType.REQUEST_RESPONSE, helloBuffer);
        Frame f2 = Frame.from(20, FrameType.COMPLETE, anotherBuffer);

        ByteBuffer b = f2.getByteBuffer();
        f.wrap(b);

        assertEquals("another", TestUtil.byteToString(f.getData()));
        assertEquals(FrameType.NEXT_COMPLETE, f.getType());
        assertEquals(20, f.getStreamId());
    }
}
