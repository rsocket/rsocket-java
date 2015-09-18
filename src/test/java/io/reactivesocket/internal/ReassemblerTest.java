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
package io.reactivesocket.internal;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.TestUtil;
import io.reactivex.subjects.ReplaySubject;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class ReassemblerTest
{
    private static final int STREAM_ID = 101;

    @Test
    public void shouldPassThroughUnfragmentedFrame()
    {
        final ReplaySubject<Payload> replaySubject = ReplaySubject.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata = "metadata";
        final String data = "data";
        final ByteBuffer metadataBuffer = TestUtil.byteBufferFromUtf8String(metadata);
        final ByteBuffer dataBuffer = TestUtil.byteBufferFromUtf8String(data);

        reassembler.onNext(Frame.Response.from(STREAM_ID, FrameType.NEXT, metadataBuffer, dataBuffer, 0));

        assertEquals(1, replaySubject.getValues().length);
        assertEquals(data, TestUtil.byteToString(replaySubject.getValue().getData()));
        assertEquals(metadata, TestUtil.byteToString(replaySubject.getValue().getMetadata()));
    }

    @Test
    public void shouldNotPassThroughFragmentedFrameIfStillMoreFollowing()
    {
        final ReplaySubject<Payload> replaySubject = ReplaySubject.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata = "metadata";
        final String data = "data";
        final ByteBuffer metadataBuffer = TestUtil.byteBufferFromUtf8String(metadata);
        final ByteBuffer dataBuffer = TestUtil.byteBufferFromUtf8String(data);

        reassembler.onNext(Frame.Response.from(STREAM_ID, FrameType.NEXT, metadataBuffer, dataBuffer, FrameHeaderFlyweight.FLAGS_RESPONSE_F));

        assertEquals(0, replaySubject.getValues().length);
    }

    @Test
    public void shouldReassembleTwoFramesWithFragmentedDataAndMetadata()
    {
        final ReplaySubject<Payload> replaySubject = ReplaySubject.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata0 = "metadata0";
        final String metadata1 = "md1";
        final String metadata = metadata0 + metadata1;
        final String data0 = "data0";
        final String data1 = "d1";
        final String data = data0 + data1;
        final ByteBuffer metadata0Buffer = TestUtil.byteBufferFromUtf8String(metadata0);
        final ByteBuffer data0Buffer = TestUtil.byteBufferFromUtf8String(data0);
        final ByteBuffer metadata1Buffer = TestUtil.byteBufferFromUtf8String(metadata1);
        final ByteBuffer data1Buffer = TestUtil.byteBufferFromUtf8String(data1);

        reassembler.onNext(Frame.Response.from(STREAM_ID, FrameType.NEXT, metadata0Buffer, data0Buffer, FrameHeaderFlyweight.FLAGS_RESPONSE_F));
        reassembler.onNext(Frame.Response.from(STREAM_ID, FrameType.NEXT, metadata1Buffer, data1Buffer, 0));

        assertEquals(1, replaySubject.getValues().length);
        assertEquals(data, TestUtil.byteToString(replaySubject.getValue().getData()));
        assertEquals(metadata, TestUtil.byteToString(replaySubject.getValue().getMetadata()));
    }

    @Test
    public void shouldReassembleTwoFramesWithFragmentedData()
    {
        final ReplaySubject<Payload> replaySubject = ReplaySubject.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata = "metadata";
        final String data0 = "data0";
        final String data1 = "d1";
        final String data = data0 + data1;
        final ByteBuffer metadataBuffer = TestUtil.byteBufferFromUtf8String(metadata);
        final ByteBuffer data0Buffer = TestUtil.byteBufferFromUtf8String(data0);
        final ByteBuffer data1Buffer = TestUtil.byteBufferFromUtf8String(data1);

        reassembler.onNext(Frame.Response.from(STREAM_ID, FrameType.NEXT, metadataBuffer, data0Buffer, FrameHeaderFlyweight.FLAGS_RESPONSE_F));
        reassembler.onNext(Frame.Response.from(STREAM_ID, FrameType.NEXT, Frame.NULL_BYTEBUFFER, data1Buffer, 0));

        assertEquals(1, replaySubject.getValues().length);
        assertEquals(data, TestUtil.byteToString(replaySubject.getValue().getData()));
        assertEquals(metadata, TestUtil.byteToString(replaySubject.getValue().getMetadata()));
    }

    @Test
    public void shouldReassembleTwoFramesWithFragmentedMetadata()
    {
        final ReplaySubject<Payload> replaySubject = ReplaySubject.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata0 = "metadata0";
        final String metadata1 = "md1";
        final String metadata = metadata0 + metadata1;
        final String data = "data";
        final ByteBuffer metadata0Buffer = TestUtil.byteBufferFromUtf8String(metadata0);
        final ByteBuffer dataBuffer = TestUtil.byteBufferFromUtf8String(data);
        final ByteBuffer metadata1Buffer = TestUtil.byteBufferFromUtf8String(metadata1);

        reassembler.onNext(Frame.Response.from(STREAM_ID, FrameType.NEXT, metadata0Buffer, dataBuffer, FrameHeaderFlyweight.FLAGS_RESPONSE_F));
        reassembler.onNext(Frame.Response.from(STREAM_ID, FrameType.NEXT, metadata1Buffer, Frame.NULL_BYTEBUFFER, 0));

        assertEquals(1, replaySubject.getValues().length);
        assertEquals(data, TestUtil.byteToString(replaySubject.getValue().getData()));
        assertEquals(metadata, TestUtil.byteToString(replaySubject.getValue().getMetadata()));
    }

    @Test
    public void shouldReassembleTwoFramesWithFragmentedDataAndMetadataWithMoreThanTwoFragments()
    {
        final ReplaySubject<Payload> replaySubject = ReplaySubject.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata0 = "metadata0";
        final String metadata1 = "md1";
        final String metadata = metadata0 + metadata1;
        final String data0 = "data0";
        final String data1 = "d1";
        final String data2 = "d2";
        final String data = data0 + data1 + data2;
        final ByteBuffer metadata0Buffer = TestUtil.byteBufferFromUtf8String(metadata0);
        final ByteBuffer data0Buffer = TestUtil.byteBufferFromUtf8String(data0);
        final ByteBuffer metadata1Buffer = TestUtil.byteBufferFromUtf8String(metadata1);
        final ByteBuffer data1Buffer = TestUtil.byteBufferFromUtf8String(data1);
        final ByteBuffer data2Buffer = TestUtil.byteBufferFromUtf8String(data2);

        reassembler.onNext(Frame.Response.from(STREAM_ID, FrameType.NEXT, metadata0Buffer, data0Buffer, FrameHeaderFlyweight.FLAGS_RESPONSE_F));
        reassembler.onNext(Frame.Response.from(STREAM_ID, FrameType.NEXT, metadata1Buffer, data1Buffer, FrameHeaderFlyweight.FLAGS_RESPONSE_F));
        reassembler.onNext(Frame.Response.from(STREAM_ID, FrameType.NEXT, Frame.NULL_BYTEBUFFER, data2Buffer, 0));

        assertEquals(1, replaySubject.getValues().length);
        assertEquals(data, TestUtil.byteToString(replaySubject.getValue().getData()));
        assertEquals(metadata, TestUtil.byteToString(replaySubject.getValue().getMetadata()));
    }

}
