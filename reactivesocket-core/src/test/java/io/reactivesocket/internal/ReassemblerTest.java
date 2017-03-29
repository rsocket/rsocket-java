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
package io.reactivesocket.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import io.reactivesocket.frame.PayloadReassembler;
import org.junit.Test;
import reactor.core.publisher.ReplayProcessor;

import java.nio.charset.StandardCharsets;

public class ReassemblerTest
{
    private static final int STREAM_ID = 101;

    @Test
    public void shouldPassThroughUnfragmentedFrame()
    {
        final ReplayProcessor<Payload> replaySubject = ReplayProcessor.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata = "metadata";
        final String data = "data";
        final ByteBuf metadataBuffer = Unpooled.copiedBuffer(metadata, StandardCharsets.UTF_8);
        final ByteBuf dataBuffer = Unpooled.copiedBuffer(data, StandardCharsets.UTF_8);

        reassembler.onNext(Frame.PayloadFrame.from(STREAM_ID, FrameType.NEXT, metadataBuffer, dataBuffer, 0));

        //assertEquals(1, replaySubject.getValues().length);
        //assertEquals(data, TestUtil.byteToString(replaySubject.getValue().getData()));
        //assertEquals(metadata, TestUtil.byteToString(replaySubject.getValue().getMetadata()));
    }

    @Test
    public void shouldNotPassThroughFragmentedFrameIfStillMoreFollowing()
    {
        final ReplayProcessor<Payload> replaySubject = ReplayProcessor.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata = "metadata";
        final String data = "data";
        final ByteBuf metadataBuffer = Unpooled.copiedBuffer(metadata, StandardCharsets.UTF_8);
        final ByteBuf dataBuffer = Unpooled.copiedBuffer(data, StandardCharsets.UTF_8);

        reassembler.onNext(Frame.PayloadFrame.from(STREAM_ID, FrameType.NEXT, metadataBuffer, dataBuffer, FrameHeaderFlyweight.FLAGS_F));

        //assertEquals(0, replaySubject.getValues().length);
    }

    @Test
    public void shouldReassembleTwoFramesWithFragmentedDataAndMetadata()
    {
        final ReplayProcessor<Payload> replaySubject = ReplayProcessor.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata0 = "metadata0";
        final String metadata1 = "md1";
        final String metadata = metadata0 + metadata1;
        final String data0 = "data0";
        final String data1 = "d1";
        final String data = data0 + data1;
        final ByteBuf metadata0Buffer = Unpooled.copiedBuffer(metadata0, StandardCharsets.UTF_8);
        final ByteBuf data0Buffer = Unpooled.copiedBuffer(data0, StandardCharsets.UTF_8);
        final ByteBuf metadata1Buffer = Unpooled.copiedBuffer(metadata1, StandardCharsets.UTF_8);
        final ByteBuf data1Buffer = Unpooled.copiedBuffer(data1, StandardCharsets.UTF_8);

        reassembler.onNext(Frame.PayloadFrame.from(STREAM_ID, FrameType.NEXT, metadata0Buffer, data0Buffer, FrameHeaderFlyweight.FLAGS_F));
        reassembler.onNext(Frame.PayloadFrame.from(STREAM_ID, FrameType.NEXT, metadata1Buffer, data1Buffer, 0));

        //assertEquals(1, replaySubject.getValues().length);
        //assertEquals(data, TestUtil.byteToString(replaySubject.getValue().getData()));
        //assertEquals(metadata, TestUtil.byteToString(replaySubject.getValue().getMetadata()));
    }

    @Test
    public void shouldReassembleTwoFramesWithFragmentedData()
    {
        final ReplayProcessor<Payload> replaySubject = ReplayProcessor.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata = "metadata";
        final String data0 = "data0";
        final String data1 = "d1";
        final String data = data0 + data1;
        final ByteBuf metadataBuffer = Unpooled.copiedBuffer(metadata, StandardCharsets.UTF_8);
        final ByteBuf data0Buffer = Unpooled.copiedBuffer(data0, StandardCharsets.UTF_8);
        final ByteBuf data1Buffer = Unpooled.copiedBuffer(data1, StandardCharsets.UTF_8);

        reassembler.onNext(Frame.PayloadFrame.from(STREAM_ID, FrameType.NEXT, metadataBuffer, data0Buffer, FrameHeaderFlyweight.FLAGS_F));
        reassembler.onNext(Frame.PayloadFrame.from(STREAM_ID, FrameType.NEXT, Unpooled.EMPTY_BUFFER, data1Buffer, 0));

        //assertEquals(1, replaySubject.getValues().length);
        //assertEquals(data, TestUtil.byteToString(replaySubject.getValue().getData()));
        //assertEquals(metadata, TestUtil.byteToString(replaySubject.getValue().getMetadata()));
    }

    @Test
    public void shouldReassembleTwoFramesWithFragmentedMetadata()
    {
        final ReplayProcessor<Payload> replaySubject = ReplayProcessor.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata0 = "metadata0";
        final String metadata1 = "md1";
        final String metadata = metadata0 + metadata1;
        final String data = "data";
        final ByteBuf metadata0Buffer = Unpooled.copiedBuffer(metadata0, StandardCharsets.UTF_8);
        final ByteBuf dataBuffer = Unpooled.copiedBuffer(data, StandardCharsets.UTF_8);
        final ByteBuf metadata1Buffer = Unpooled.copiedBuffer(metadata1, StandardCharsets.UTF_8);

        reassembler.onNext(Frame.PayloadFrame.from(STREAM_ID, FrameType.NEXT, metadata0Buffer, dataBuffer, FrameHeaderFlyweight.FLAGS_F));
        reassembler.onNext(Frame.PayloadFrame.from(STREAM_ID, FrameType.NEXT, metadata1Buffer, Unpooled.EMPTY_BUFFER, 0));

        //assertEquals(1, replaySubject.getValues().length);
        //assertEquals(data, TestUtil.byteToString(replaySubject.getValue().getData()));
        //assertEquals(metadata, TestUtil.byteToString(replaySubject.getValue().getMetadata()));
    }

    @Test
    public void shouldReassembleTwoFramesWithFragmentedDataAndMetadataWithMoreThanTwoFragments()
    {
        final ReplayProcessor<Payload> replaySubject = ReplayProcessor.create();
        final PayloadReassembler reassembler = PayloadReassembler.with(replaySubject);
        final String metadata0 = "metadata0";
        final String metadata1 = "md1";
        final String metadata = metadata0 + metadata1;
        final String data0 = "data0";
        final String data1 = "d1";
        final String data2 = "d2";
        final String data = data0 + data1 + data2;
        final ByteBuf metadata0Buffer = Unpooled.copiedBuffer(metadata0, StandardCharsets.UTF_8);
        final ByteBuf data0Buffer = Unpooled.copiedBuffer(data0, StandardCharsets.UTF_8);
        final ByteBuf metadata1Buffer = Unpooled.copiedBuffer(metadata1, StandardCharsets.UTF_8);
        final ByteBuf data1Buffer = Unpooled.copiedBuffer(data1, StandardCharsets.UTF_8);
        final ByteBuf data2Buffer = Unpooled.copiedBuffer(data2, StandardCharsets.UTF_8);

        reassembler.onNext(Frame.PayloadFrame.from(STREAM_ID, FrameType.NEXT, metadata0Buffer, data0Buffer, FrameHeaderFlyweight.FLAGS_F));
        reassembler.onNext(Frame.PayloadFrame.from(STREAM_ID, FrameType.NEXT, metadata1Buffer, data1Buffer, FrameHeaderFlyweight.FLAGS_F));
        reassembler.onNext(Frame.PayloadFrame.from(STREAM_ID, FrameType.NEXT, Unpooled.EMPTY_BUFFER, data2Buffer, 0));

        //assertEquals(1, replaySubject.getValues().length);
        //assertEquals(data, TestUtil.byteToString(replaySubject.getValue().getData()));
        //assertEquals(metadata, TestUtil.byteToString(replaySubject.getValue().getMetadata()));
    }

}