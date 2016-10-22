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

import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.TestUtil;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import io.reactivesocket.frame.PayloadFragmenter;

import org.junit.Test;

import static org.junit.Assert.*;

public class FragmenterTest
{
    private static final int METADATA_MTU = 256;
    private static final int DATA_MTU = 256;
    private static final int STREAM_ID = 101;
    private static final int REQUEST_N = 102;

    @Test
    public void shouldPassThroughUnfragmentedResponse()
    {
        final PayloadFragmenter fragmenter = new PayloadFragmenter(METADATA_MTU, DATA_MTU);
        final Payload payload = TestUtil.utf8EncodedPayload("response data", "response metadata");

        fragmenter.resetForResponse(STREAM_ID, payload);

        assertTrue(fragmenter.hasNext());
        final Frame frame1 = fragmenter.next();

        assertEquals("response data", TestUtil.byteToString(frame1.getData()));
        assertEquals("response metadata", TestUtil.byteToString(frame1.getMetadata()));
        assertEquals(0, (frame1.flags() & FrameHeaderFlyweight.FLAGS_RESPONSE_F));
        assertFalse(fragmenter.hasNext());
    }

    @Test
    public void shouldHandleFragmentedResponseData()
    {
        final String responseData0 = "response ";
        final String responseData1 = "data";
        final String responseData = responseData0 + responseData1;
        final PayloadFragmenter fragmenter = new PayloadFragmenter(METADATA_MTU, responseData0.length());
        final Payload payload = TestUtil.utf8EncodedPayload(responseData, "response metadata");

        fragmenter.resetForResponse(STREAM_ID, payload);

        assertTrue(fragmenter.hasNext());
        final Frame frame1 = fragmenter.next();

        assertEquals(responseData0, TestUtil.byteToString(frame1.getData()));
        assertEquals("response metadata", TestUtil.byteToString(frame1.getMetadata()));
        assertEquals(FrameHeaderFlyweight.FLAGS_RESPONSE_F, (frame1.flags() & FrameHeaderFlyweight.FLAGS_RESPONSE_F));

        assertTrue(fragmenter.hasNext());
        final Frame frame2 = fragmenter.next();

        assertEquals(responseData1, TestUtil.byteToString(frame2.getData()));
        assertEquals("", TestUtil.byteToString(frame2.getMetadata()));
        assertEquals(0, (frame2.flags() & FrameHeaderFlyweight.FLAGS_RESPONSE_F));
        assertFalse(fragmenter.hasNext());
    }

    @Test
    public void shouldHandleFragmentedResponseMetadata()
    {
        final String responseMetadata0 = "response ";
        final String responseMetadata1 = "metadata";
        final String responseMetadata = responseMetadata0 + responseMetadata1;
        final PayloadFragmenter fragmenter = new PayloadFragmenter(responseMetadata0.length(), DATA_MTU);
        final Payload payload = TestUtil.utf8EncodedPayload("response data", responseMetadata);

        fragmenter.resetForResponse(STREAM_ID, payload);

        assertTrue(fragmenter.hasNext());
        final Frame frame1 = fragmenter.next();

        assertEquals("response data", TestUtil.byteToString(frame1.getData()));
        assertEquals(responseMetadata0, TestUtil.byteToString(frame1.getMetadata()));
        assertEquals(FrameHeaderFlyweight.FLAGS_RESPONSE_F, (frame1.flags() & FrameHeaderFlyweight.FLAGS_RESPONSE_F));

        assertTrue(fragmenter.hasNext());
        final Frame frame2 = fragmenter.next();

        assertEquals("", TestUtil.byteToString(frame2.getData()));
        assertEquals(responseMetadata1, TestUtil.byteToString(frame2.getMetadata()));
        assertEquals(0, (frame2.flags() & FrameHeaderFlyweight.FLAGS_RESPONSE_F));
        assertFalse(fragmenter.hasNext());
    }

    @Test
    public void shouldHandleFragmentedResponseMetadataAndData()
    {
        final String responseMetadata0 = "response ";
        final String responseMetadata1 = "metadata";
        final String responseMetadata = responseMetadata0 + responseMetadata1;
        final String responseData0 = "response ";
        final String responseData1 = "data";
        final String responseData = responseData0 + responseData1;
        final PayloadFragmenter fragmenter = new PayloadFragmenter(responseMetadata0.length(), responseData0.length());
        final Payload payload = TestUtil.utf8EncodedPayload(responseData, responseMetadata);

        fragmenter.resetForResponse(STREAM_ID, payload);

        assertTrue(fragmenter.hasNext());
        final Frame frame1 = fragmenter.next();

        assertEquals(responseData0, TestUtil.byteToString(frame1.getData()));
        assertEquals(responseMetadata0, TestUtil.byteToString(frame1.getMetadata()));
        assertEquals(FrameHeaderFlyweight.FLAGS_RESPONSE_F, (frame1.flags() & FrameHeaderFlyweight.FLAGS_RESPONSE_F));

        assertTrue(fragmenter.hasNext());
        final Frame frame2 = fragmenter.next();

        assertEquals(responseData1, TestUtil.byteToString(frame2.getData()));
        assertEquals(responseMetadata1, TestUtil.byteToString(frame2.getMetadata()));
        assertEquals(0, (frame2.flags() & FrameHeaderFlyweight.FLAGS_RESPONSE_F));
        assertFalse(fragmenter.hasNext());
    }

    @Test
    public void shouldHandleFragmentedResponseMetadataAndDataWithMoreThanTwoFragments()
    {
        final String responseMetadata0 = "response ";
        final String responseMetadata1 = "metadata";
        final String responseMetadata = responseMetadata0 + responseMetadata1;
        final String responseData0 = "response ";
        final String responseData1 = "data     ";
        final String responseData2 = "and more";
        final String responseData = responseData0 + responseData1 + responseData2;
        final PayloadFragmenter fragmenter = new PayloadFragmenter(responseMetadata0.length(), responseData0.length());
        final Payload payload = TestUtil.utf8EncodedPayload(responseData, responseMetadata);

        fragmenter.resetForResponse(STREAM_ID, payload);

        assertTrue(fragmenter.hasNext());
        final Frame frame1 = fragmenter.next();

        assertEquals(responseData0, TestUtil.byteToString(frame1.getData()));
        assertEquals(responseMetadata0, TestUtil.byteToString(frame1.getMetadata()));
        assertEquals(FrameHeaderFlyweight.FLAGS_RESPONSE_F, (frame1.flags() & FrameHeaderFlyweight.FLAGS_RESPONSE_F));

        assertTrue(fragmenter.hasNext());
        final Frame frame2 = fragmenter.next();

        assertEquals(responseData1, TestUtil.byteToString(frame2.getData()));
        assertEquals(responseMetadata1, TestUtil.byteToString(frame2.getMetadata()));
        assertEquals(FrameHeaderFlyweight.FLAGS_RESPONSE_F, (frame2.flags() & FrameHeaderFlyweight.FLAGS_RESPONSE_F));

        assertTrue(fragmenter.hasNext());
        final Frame frame3 = fragmenter.next();

        assertEquals(responseData2, TestUtil.byteToString(frame3.getData()));
        assertEquals("", TestUtil.byteToString(frame3.getMetadata()));
        assertEquals(0, (frame3.flags() & FrameHeaderFlyweight.FLAGS_RESPONSE_F));
        assertFalse(fragmenter.hasNext());
    }

    @Test
    public void shouldHandleFragmentedRequestChannelMetadataAndData()
    {
        final String requestMetadata0 = "request ";
        final String requestMetadata1 = "metadata";
        final String requestMetadata = requestMetadata0 + requestMetadata1;
        final String requestData0 = "request ";
        final String requestData1 = "data";
        final String requestData = requestData0 + requestData1;
        final PayloadFragmenter fragmenter = new PayloadFragmenter(requestMetadata0.length(), requestData0.length());
        final Payload payload = TestUtil.utf8EncodedPayload(requestData, requestMetadata);

        fragmenter.resetForRequestChannel(STREAM_ID, payload, REQUEST_N);

        assertTrue(fragmenter.hasNext());
        final Frame frame1 = fragmenter.next();

        assertEquals(requestData0, TestUtil.byteToString(frame1.getData()));
        assertEquals(requestMetadata0, TestUtil.byteToString(frame1.getMetadata()));
        assertEquals(FrameHeaderFlyweight.FLAGS_REQUEST_CHANNEL_F, (frame1.flags() & FrameHeaderFlyweight.FLAGS_REQUEST_CHANNEL_F));

        assertTrue(fragmenter.hasNext());
        final Frame frame2 = fragmenter.next();

        assertEquals(requestData1, TestUtil.byteToString(frame2.getData()));
        assertEquals(requestMetadata1, TestUtil.byteToString(frame2.getMetadata()));
        assertEquals(0, (frame2.flags() & FrameHeaderFlyweight.FLAGS_REQUEST_CHANNEL_F));
        assertFalse(fragmenter.hasNext());
    }
}
