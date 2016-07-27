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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBufUtil;
import io.reactivesocket.exceptions.Exceptions;
import io.reactivesocket.exceptions.RejectedException;
import io.reactivesocket.internal.frame.SetupFrameFlyweight;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.reactivesocket.internal.frame.ErrorFrameFlyweight.REJECTED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

@RunWith(Theories.class)
public class FrameTest {
    private static Payload createPayload(final ByteBuffer metadata, final ByteBuffer data) {
        return new Payload() {
            public ByteBuffer getData() {
                return data;
            }

            public ByteBuffer getMetadata() {
                return metadata;
            }
        };
    }

    @DataPoint
    public static final int ZERO_OFFSET = 0;

    @DataPoint
    public static final int NON_ZERO_OFFSET = 127;

    private static final UnsafeBuffer reusableMutableDirectBuffer =
            new UnsafeBuffer(ByteBuffer.allocate(1024));
    private static final Frame reusableFrame = Frame.allocate(reusableMutableDirectBuffer);

    @Test
    public void testWriteThenRead() {
        final ByteBuffer helloBuffer = TestUtil.byteBufferFromUtf8String("hello");
        final Payload payload = createPayload(Frame.NULL_BYTEBUFFER, helloBuffer);

        Frame f = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, payload, 1);

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
        final ByteBuffer doneBuffer = TestUtil.byteBufferFromUtf8String("done");
        final Payload payload = createPayload(Frame.NULL_BYTEBUFFER, helloBuffer);

        Frame f = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, payload, 1);

        f.wrap(2, FrameType.COMPLETE, doneBuffer);
        assertEquals("done", TestUtil.byteToString(f.getData()));
        assertEquals(FrameType.NEXT_COMPLETE, f.getType());
        assertEquals(2, f.getStreamId());
    }

    @Test
    public void testWrapBytes() {
        final ByteBuffer helloBuffer = TestUtil.byteBufferFromUtf8String("hello");
        final ByteBuffer anotherBuffer = TestUtil.byteBufferFromUtf8String("another");
        final Payload payload = createPayload(Frame.NULL_BYTEBUFFER, helloBuffer);
        final Payload anotherPayload = createPayload(Frame.NULL_BYTEBUFFER, anotherBuffer);

        Frame f = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, payload, 1);
        Frame f2 = Frame.Response.from(20, FrameType.COMPLETE, anotherPayload);

        ByteBuffer b = f2.getByteBuffer();
        f.wrap(b, 0);

        assertEquals("another", TestUtil.byteToString(f.getData()));
        assertEquals(FrameType.NEXT_COMPLETE, f.getType());
        assertEquals(20, f.getStreamId());
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataPlusMetadataForRequestResponse(final int offset) {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final ByteBuffer requestMetadata = TestUtil.byteBufferFromUtf8String("request metadata");
        final Payload payload = createPayload(requestMetadata, requestData);

        Frame encodedFrame = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, payload, 1);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals(FrameType.REQUEST_RESPONSE, reusableFrame.getType());
        assertEquals(1, reusableFrame.getStreamId());
        assertEquals("request data", TestUtil.byteToString(reusableFrame.getData()));
        assertEquals("request metadata", TestUtil.byteToString(reusableFrame.getMetadata()));

        assertEquals(
                "0000002c0004400000000001",
                ByteBufUtil.hexDump(encodedFrame.getByteBuffer().array(), 0, 12));
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataPlusMetadataForFireAndForget(final int offset) {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final ByteBuffer requestMetadata = TestUtil.byteBufferFromUtf8String("request metadata");
        final Payload payload = createPayload(requestMetadata, requestData);

        Frame encodedFrame = Frame.Request.from(1, FrameType.FIRE_AND_FORGET, payload, 0);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals("request data", TestUtil.byteToString(reusableFrame.getData()));
        assertEquals("request metadata", TestUtil.byteToString(reusableFrame.getMetadata()));
        assertEquals(FrameType.FIRE_AND_FORGET, reusableFrame.getType());
        assertEquals(1, reusableFrame.getStreamId());

        assertEquals(
                "0000002c0005400000000001",
                ByteBufUtil.hexDump(encodedFrame.getByteBuffer().array(), 0, 12));
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataPlusMetadataForRequestStream(final int offset) {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final ByteBuffer requestMetadata = TestUtil.byteBufferFromUtf8String("request metadata");
        final Payload payload = createPayload(requestMetadata, requestData);

        Frame encodedFrame = Frame.Request.from(1, FrameType.REQUEST_STREAM, payload, 128);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals("request data", TestUtil.byteToString(reusableFrame.getData()));
        assertEquals("request metadata", TestUtil.byteToString(reusableFrame.getMetadata()));
        assertEquals(FrameType.REQUEST_STREAM, reusableFrame.getType());
        assertEquals(1, reusableFrame.getStreamId());
        assertEquals(128, Frame.Request.initialRequestN(reusableFrame));

        assertEquals(
                "000000300006480000000001",
                ByteBufUtil.hexDump(encodedFrame.getByteBuffer().array(), 0, 12));
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataPlusMetadataForRequestSubscription(final int offset) {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final ByteBuffer requestMetadata = TestUtil.byteBufferFromUtf8String("request metadata");
        final Payload payload = createPayload(requestMetadata, requestData);

        Frame encodedFrame = Frame.Request.from(1, FrameType.REQUEST_SUBSCRIPTION, payload, 128);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals("request data", TestUtil.byteToString(reusableFrame.getData()));
        assertEquals("request metadata", TestUtil.byteToString(reusableFrame.getMetadata()));
        assertEquals(FrameType.REQUEST_SUBSCRIPTION, reusableFrame.getType());
        assertEquals(1, reusableFrame.getStreamId());
        assertEquals(128, Frame.Request.initialRequestN(reusableFrame));

        assertEquals(
                "000000300007480000000001",
                ByteBufUtil.hexDump(encodedFrame.getByteBuffer().array(), 0, 12));
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataPlusMetadataForResponse(final int offset) {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("response data");
        final ByteBuffer requestMetadata = TestUtil.byteBufferFromUtf8String("response metadata");
        final Payload payload = createPayload(requestMetadata, requestData);

        Frame encodedFrame = Frame.Response.from(1, FrameType.RESPONSE, payload);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals("response data", TestUtil.byteToString(reusableFrame.getData()));
        assertEquals("response metadata", TestUtil.byteToString(reusableFrame.getMetadata()));
        assertEquals(FrameType.NEXT, reusableFrame.getType());
        assertEquals(1, reusableFrame.getStreamId());

        assertEquals(
                "0000002e000b400000000001",
                ByteBufUtil.hexDump(encodedFrame.getByteBuffer().array(), 0, 12));
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataWithoutMetadataForRequestResponse(final int offset) {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final Payload payload = createPayload(Frame.NULL_BYTEBUFFER, requestData);

        Frame encodedFrame = Frame.Request.from(1, FrameType.REQUEST_RESPONSE, payload, 1);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals("request data", TestUtil.byteToString(reusableFrame.getData()));

        final ByteBuffer metadataBuffer = reusableFrame.getMetadata();
        assertEquals(0, metadataBuffer.remaining());
        assertEquals(FrameType.REQUEST_RESPONSE, reusableFrame.getType());
        assertEquals(1, reusableFrame.getStreamId());
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataWithoutMetadataForFireAndForget(final int offset) {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final Payload payload = createPayload(Frame.NULL_BYTEBUFFER, requestData);

        Frame encodedFrame = Frame.Request.from(1, FrameType.FIRE_AND_FORGET, payload, 0);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals("request data", TestUtil.byteToString(reusableFrame.getData()));

        final ByteBuffer metadataBuffer = reusableFrame.getMetadata();
        assertEquals(0, metadataBuffer.remaining());
        assertEquals(FrameType.FIRE_AND_FORGET, reusableFrame.getType());
        assertEquals(1, reusableFrame.getStreamId());
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataWithoutMetadataForRequestStream(final int offset) {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final Payload payload = createPayload(Frame.NULL_BYTEBUFFER, requestData);

        Frame encodedFrame = Frame.Request.from(1, FrameType.REQUEST_STREAM, payload, 128);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals("request data", TestUtil.byteToString(reusableFrame.getData()));

        final ByteBuffer metadataBuffer = reusableFrame.getMetadata();
        assertEquals(0, metadataBuffer.remaining());
        assertEquals(FrameType.REQUEST_STREAM, reusableFrame.getType());
        assertEquals(1, reusableFrame.getStreamId());
        assertEquals(128, Frame.Request.initialRequestN(reusableFrame));
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataWithoutMetadataForRequestSubscription(final int offset) {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("request data");
        final Payload payload = createPayload(Frame.NULL_BYTEBUFFER, requestData);

        Frame encodedFrame = Frame.Request.from(1, FrameType.REQUEST_SUBSCRIPTION, payload, 128);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals("request data", TestUtil.byteToString(reusableFrame.getData()));

        final ByteBuffer metadataBuffer = reusableFrame.getMetadata();
        assertEquals(0, metadataBuffer.remaining());
        assertEquals(FrameType.REQUEST_SUBSCRIPTION, reusableFrame.getType());
        assertEquals(1, reusableFrame.getStreamId());
        assertEquals(128, Frame.Request.initialRequestN(reusableFrame));
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataWithoutMetadataForResponse(final int offset) {
        final ByteBuffer requestData = TestUtil.byteBufferFromUtf8String("response data");
        final Payload payload = createPayload(Frame.NULL_BYTEBUFFER, requestData);

        Frame encodedFrame = Frame.Response.from(1, FrameType.RESPONSE, payload);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals("response data", TestUtil.byteToString(reusableFrame.getData()));

        final ByteBuffer metadataBuffer = reusableFrame.getMetadata();
        assertEquals(0, metadataBuffer.remaining());
        assertEquals(FrameType.NEXT, reusableFrame.getType());
        assertEquals(1, reusableFrame.getStreamId());
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataPlusMetadataForSetup(final int offset) {
        final int flags = SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE
                | SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;
        final int version = SetupFrameFlyweight.CURRENT_VERSION;
        final int keepaliveInterval = 1001;
        final int maxLifetime = keepaliveInterval * 5;
        final String metadataMimeType = "application/json";
        final String dataMimeType = "application/cbor";
        final ByteBuffer setupData = TestUtil.byteBufferFromUtf8String("setup data");
        final ByteBuffer setupMetadata = TestUtil.byteBufferFromUtf8String("setup metadata");

        Frame encodedFrame =
                Frame.Setup.from(flags, keepaliveInterval, maxLifetime, metadataMimeType, dataMimeType,
                        new Payload() {
                            public ByteBuffer getData() {
                                return setupData;
                            }

                            public ByteBuffer getMetadata() {
                                return setupMetadata;
                            }
                        });
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals(FrameType.SETUP, reusableFrame.getType());
        assertEquals(flags, Frame.Setup.getFlags(reusableFrame));
        assertEquals(version, Frame.Setup.version(reusableFrame));
        assertEquals(keepaliveInterval, Frame.Setup.keepaliveInterval(reusableFrame));
        assertEquals(maxLifetime, Frame.Setup.maxLifetime(reusableFrame));
        assertEquals(metadataMimeType, Frame.Setup.metadataMimeType(reusableFrame));
        assertEquals(dataMimeType, Frame.Setup.dataMimeType(reusableFrame));
        assertEquals("setup data", TestUtil.byteToString(reusableFrame.getData()));
        assertEquals("setup metadata", TestUtil.byteToString(reusableFrame.getMetadata()));
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataWithoutMetadataForSetup(final int offset) {
        final int flags = SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE
                | SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;
        final int version = SetupFrameFlyweight.CURRENT_VERSION;
        final int keepaliveInterval = 1001;
        final int maxLifetime = keepaliveInterval * 5;
        final String metadataMimeType = "application/json";
        final String dataMimeType = "application/cbor";
        final ByteBuffer setupData = TestUtil.byteBufferFromUtf8String("setup data");

        Frame encodedFrame =
                Frame.Setup.from(flags, keepaliveInterval, maxLifetime, metadataMimeType, dataMimeType,
                        new Payload() {
                            public ByteBuffer getData() {
                                return setupData;
                            }

                            public ByteBuffer getMetadata() {
                                return Frame.NULL_BYTEBUFFER;
                            }
                        });
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals(FrameType.SETUP, reusableFrame.getType());
        assertEquals(flags, Frame.Setup.getFlags(reusableFrame));
        assertEquals(version, Frame.Setup.version(reusableFrame));
        assertEquals(keepaliveInterval, Frame.Setup.keepaliveInterval(reusableFrame));
        assertEquals(maxLifetime, Frame.Setup.maxLifetime(reusableFrame));
        assertEquals(metadataMimeType, Frame.Setup.metadataMimeType(reusableFrame));
        assertEquals(dataMimeType, Frame.Setup.dataMimeType(reusableFrame));
        assertEquals("setup data", TestUtil.byteToString(reusableFrame.getData()));
        assertEquals(Frame.NULL_BYTEBUFFER, reusableFrame.getMetadata());
    }

    @Test
    @Theory
    public void shouldFormCorrectlyWithoutDataNorMetadataForSetup(final int offset) {
        final int flags = SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE
                | SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;
        final int version = SetupFrameFlyweight.CURRENT_VERSION;
        final int keepaliveInterval = 1001;
        final int maxLifetime = keepaliveInterval * 5;
        final String metadataMimeType = "application/json";
        final String dataMimeType = "application/cbor";

        Frame encodedFrame =
                Frame.Setup.from(flags, keepaliveInterval, maxLifetime, metadataMimeType, dataMimeType,
                        new Payload() {
                            public ByteBuffer getData() {
                                return Frame.NULL_BYTEBUFFER;
                            }

                            public ByteBuffer getMetadata() {
                                return Frame.NULL_BYTEBUFFER;
                            }
                        });
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals(FrameType.SETUP, reusableFrame.getType());
        assertEquals(flags, Frame.Setup.getFlags(reusableFrame));
        assertEquals(version, Frame.Setup.version(reusableFrame));
        assertEquals(keepaliveInterval, Frame.Setup.keepaliveInterval(reusableFrame));
        assertEquals(maxLifetime, Frame.Setup.maxLifetime(reusableFrame));
        assertEquals(metadataMimeType, Frame.Setup.metadataMimeType(reusableFrame));
        assertEquals(dataMimeType, Frame.Setup.dataMimeType(reusableFrame));
        assertEquals(Frame.NULL_BYTEBUFFER, reusableFrame.getData());
        assertEquals(Frame.NULL_BYTEBUFFER, reusableFrame.getMetadata());

        assertEquals(
                "0000003a000130000000000000000000000003e90000138d",
                ByteBufUtil.hexDump(encodedFrame.getByteBuffer().array(), 0, 24));
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataPlusMetadataForError(final int offset) {
        final int streamId = 24;
        final Throwable exception = new RejectedException("test");
        final String data = "error data";
        final String metadata = "error metadata";
        final ByteBuffer dataByteBuffer = ByteBuffer.wrap(data.getBytes(UTF_8));
        final ByteBuffer metadataByteBuffer = ByteBuffer.wrap(metadata.getBytes(UTF_8));

        Frame encodedFrame = Frame.Error.from(streamId, exception, metadataByteBuffer, dataByteBuffer);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals(FrameType.ERROR, reusableFrame.getType());
        assertEquals(REJECTED, Frame.Error.errorCode(reusableFrame));
        assertEquals(data, TestUtil.byteToString(reusableFrame.getData()));
        assertEquals(metadata, TestUtil.byteToString(reusableFrame.getMetadata()));

        assertEquals(
                "0000002c000c40000000001800000202",
                ByteBufUtil.hexDump(encodedFrame.getByteBuffer().array(), 0, 16));
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataWithThrowableForError(final int offset) {
        final int errorCode = 42;
        final String metadata = "my metadata";
        final String exMessage = "exception message";

        Frame encodedFrame = Frame.Error.from(
                errorCode,
                new Exception(exMessage),
                TestUtil.byteBufferFromUtf8String(metadata)
        );
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals(FrameType.ERROR, reusableFrame.getType());
        assertEquals(exMessage, TestUtil.byteToString(reusableFrame.getData()));
        assertEquals(TestUtil.byteBufferFromUtf8String(metadata), reusableFrame.getMetadata());

        final Throwable throwable = Exceptions.from(encodedFrame);
        assertEquals(exMessage, throwable.getMessage());
    }

    @Test
    @Theory
    public void shouldReturnCorrectDataWithoutMetadataForError(final int offset) {
        final int errorCode = 42;
        final String metadata = "metadata";
        final String data = "error data";

        Frame encodedFrame = Frame.Error.from(
                errorCode,
                new Exception("my exception"),
                TestUtil.byteBufferFromUtf8String(metadata),
                TestUtil.byteBufferFromUtf8String(data)
        );
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals(FrameType.ERROR, reusableFrame.getType());
        assertEquals(data, TestUtil.byteToString(reusableFrame.getData()));
        assertEquals(metadata, TestUtil.byteToString(reusableFrame.getMetadata()));
    }

    @Test
    @Theory
    public void shouldFormCorrectlyForRequestN(final int offset) {
        final int n = 128;
        final Frame encodedFrame = Frame.RequestN.from(1, n);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals(FrameType.REQUEST_N, reusableFrame.getType());
        assertEquals(n, Frame.RequestN.requestN(reusableFrame));
        assertEquals(Frame.NULL_BYTEBUFFER, reusableFrame.getData());
        assertEquals(Frame.NULL_BYTEBUFFER, reusableFrame.getMetadata());

        assertEquals(
                "00000010000900000000000100000080",
                ByteBufUtil.hexDump(encodedFrame.getByteBuffer().array(), 0, 16));
    }

    @Test
    @Theory
    public void shouldFormCorrectlyWithoutMetadataForLease(final int offset) {
        final int ttl = (int) TimeUnit.SECONDS.toMillis(8);
        final int numberOfRequests = 16;
        final Frame encodedFrame = Frame.Lease.from(ttl, numberOfRequests, Frame.NULL_BYTEBUFFER);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals(0, reusableFrame.getStreamId());
        assertEquals(FrameType.LEASE, reusableFrame.getType());
        assertEquals(ttl, Frame.Lease.ttl(reusableFrame));
        assertEquals(numberOfRequests, Frame.Lease.numberOfRequests(reusableFrame));
        assertEquals(Frame.NULL_BYTEBUFFER, reusableFrame.getData());
        assertEquals(Frame.NULL_BYTEBUFFER, reusableFrame.getMetadata());

        assertEquals(
                "00000014000200000000000000001f4000000010",
                ByteBufUtil.hexDump(encodedFrame.getByteBuffer().array(), 0, 20));
    }

    @Test
    @Theory
    public void shouldFormCorrectlyWithMetadataForLease(final int offset) {
        final int ttl = (int) TimeUnit.SECONDS.toMillis(8);
        final int numberOfRequests = 16;
        final ByteBuffer leaseMetadata = TestUtil.byteBufferFromUtf8String("lease metadata");

        final Frame encodedFrame = Frame.Lease.from(ttl, numberOfRequests, leaseMetadata);
        TestUtil.copyFrame(reusableMutableDirectBuffer, offset, encodedFrame);
        reusableFrame.wrap(reusableMutableDirectBuffer, offset);

        assertEquals(0, reusableFrame.getStreamId());
        assertEquals(FrameType.LEASE, reusableFrame.getType());
        assertEquals(ttl, Frame.Lease.ttl(reusableFrame));
        assertEquals(numberOfRequests, Frame.Lease.numberOfRequests(reusableFrame));
        assertEquals(Frame.NULL_BYTEBUFFER, reusableFrame.getData());
        assertEquals("lease metadata", TestUtil.byteToString(reusableFrame.getMetadata()));
    }
}
