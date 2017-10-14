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
package io.rsocket;

import static io.rsocket.frame.FrameHeaderFlyweight.FLAGS_M;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ResourceLeakDetector;
import io.rsocket.frame.ErrorFrameFlyweight;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.KeepaliveFrameFlyweight;
import io.rsocket.frame.LeaseFrameFlyweight;
import io.rsocket.frame.RequestFrameFlyweight;
import io.rsocket.frame.RequestNFrameFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.frame.VersionFlyweight;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a Frame sent over a {@link DuplexConnection}.
 *
 * <p>This provides encoding, decoding and field accessors.
 */
public class Frame implements ByteBufHolder {
  public static final ByteBuffer NULL_BYTEBUFFER = ByteBuffer.allocateDirect(0);

  private static final Recycler<Frame> RECYCLER =
      new Recycler<Frame>() {
        protected Frame newObject(Handle<Frame> handle) {
          return new Frame(handle);
        }
      };

  private final Handle<Frame> handle;
  private @Nullable ByteBuf content;

  private Frame(final Handle<Frame> handle) {
    this.handle = handle;
  }

  /** Clear and recycle this instance. */
  private void recycle() {
    content = null;
    handle.recycle(this);
  }

  /** Return the content which is held by this {@link Frame}. */
  @Override
  public ByteBuf content() {
    if (content.refCnt() <= 0) {
      throw new IllegalReferenceCountException(content.refCnt());
    }
    return content;
  }

  /** Creates a deep copy of this {@link Frame}. */
  @Override
  public Frame copy() {
    return replace(content.copy());
  }

  /**
   * Duplicates this {@link Frame}. Be aware that this will not automatically call {@link
   * #retain()}.
   */
  @Override
  public Frame duplicate() {
    return replace(content.duplicate());
  }

  /**
   * Duplicates this {@link Frame}. This method returns a retained duplicate unlike {@link
   * #duplicate()}.
   *
   * @see ByteBuf#retainedDuplicate()
   */
  @Override
  public Frame retainedDuplicate() {
    return replace(content.retainedDuplicate());
  }

  /** Returns a new {@link Frame} which contains the specified {@code content}. */
  @Override
  public Frame replace(ByteBuf content) {
    return from(content);
  }

  /**
   * Returns the reference count of this object. If {@code 0}, it means this object has been
   * deallocated.
   */
  @Override
  public int refCnt() {
    return content.refCnt();
  }

  /** Increases the reference count by {@code 1}. */
  @Override
  public Frame retain() {
    content.retain();
    return this;
  }

  /** Increases the reference count by the specified {@code increment}. */
  @Override
  public Frame retain(int increment) {
    content.retain(increment);
    return this;
  }

  /**
   * Records the current access location of this object for debugging purposes. If this object is
   * determined to be leaked, the information recorded by this operation will be provided to you via
   * {@link ResourceLeakDetector}. This method is a shortcut to {@link #touch(Object) touch(null)}.
   */
  @Override
  public Frame touch() {
    content.touch();
    return this;
  }

  /**
   * Records the current access location of this object with an additional arbitrary information for
   * debugging purposes. If this object is determined to be leaked, the information recorded by this
   * operation will be provided to you via {@link ResourceLeakDetector}.
   */
  @Override
  public Frame touch(@Nullable Object hint) {
    content.touch(hint);
    return this;
  }

  /**
   * Decreases the reference count by {@code 1} and deallocates this object if the reference count
   * reaches at {@code 0}.
   *
   * @return {@code true} if and only if the reference count became {@code 0} and this object has
   *     been deallocated
   */
  @Override
  public boolean release() {
    if (content.release()) {
      recycle();
      return true;
    }
    return false;
  }

  /**
   * Decreases the reference count by the specified {@code decrement} and deallocates this object if
   * the reference count reaches at {@code 0}.
   *
   * @return {@code true} if and only if the reference count became {@code 0} and this object has
   *     been deallocated
   */
  @Override
  public boolean release(int decrement) {
    if (content.release(decrement)) {
      recycle();
      return true;
    }
    return false;
  }

  /**
   * Return {@link ByteBuffer} that is a {@link ByteBuffer#slice()} for the frame metadata
   *
   * <p>If no metadata is present, the ByteBuffer will have 0 capacity.
   *
   * @return ByteBuffer containing the content
   */
  public ByteBuffer getMetadata() {
    final ByteBuf metadata = FrameHeaderFlyweight.sliceFrameMetadata(content);
    if (metadata == null) {
      return NULL_BYTEBUFFER;
    } else if (metadata.readableBytes() > 0) {
      final ByteBuffer buffer = ByteBuffer.allocateDirect(metadata.readableBytes());
      metadata.readBytes(buffer);
      buffer.flip();
      return buffer;
    } else {
      return NULL_BYTEBUFFER;
    }
  }

  /**
   * Return {@link ByteBuffer} that is a {@link ByteBuffer#slice()} for the frame data
   *
   * <p>If no data is present, the ByteBuffer will have 0 capacity.
   *
   * @return ByteBuffer containing the data
   */
  public ByteBuffer getData() {
    final ByteBuf data = FrameHeaderFlyweight.sliceFrameData(content);
    if (data.readableBytes() > 0) {
      final ByteBuffer buffer = ByteBuffer.allocateDirect(data.readableBytes());
      data.readBytes(buffer);
      buffer.flip();
      return buffer;
    } else {
      return NULL_BYTEBUFFER;
    }
  }

  /**
   * Return frame stream identifier
   *
   * @return frame stream identifier
   */
  public int getStreamId() {
    return FrameHeaderFlyweight.streamId(content);
  }

  /**
   * Return frame {@link FrameType}
   *
   * @return frame type
   */
  public FrameType getType() {
    return FrameHeaderFlyweight.frameType(content);
  }

  /**
   * Return the flags field for the frame
   *
   * @return frame flags field value
   */
  public int flags() {
    return FrameHeaderFlyweight.flags(content);
  }

  /**
   * Acquire a free Frame backed by given ByteBuf
   *
   * @param content to use as backing buffer
   * @return frame
   */
  public static Frame from(final ByteBuf content) {
    final Frame frame = RECYCLER.get();
    frame.content = content;

    return frame;
  }

  public static boolean isFlagSet(int flags, int checkedFlag) {
    return (flags & checkedFlag) == checkedFlag;
  }

  public static int setFlag(int current, int toSet) {
    return current | toSet;
  }

  public boolean hasMetadata() {
    return Frame.isFlagSet(this.flags(), FLAGS_M);
  }

  public String getDataUtf8() {
    return StandardCharsets.UTF_8.decode(getData()).toString();
  }

  /* TODO:
   *
   * fromRequest(type, id, payload)
   * fromKeepalive(ByteBuf content)
   *
   */

  // SETUP specific getters
  public static class Setup {

    private Setup() {}

    public static Frame from(
        int flags,
        int keepaliveInterval,
        int maxLifetime,
        String metadataMimeType,
        String dataMimeType,
        Payload payload) {
      final ByteBuf metadata =
          payload.hasMetadata()
              ? Unpooled.wrappedBuffer(payload.getMetadata())
              : Unpooled.EMPTY_BUFFER;
      final ByteBuf data =
          payload.getData() != null
              ? Unpooled.wrappedBuffer(payload.getData())
              : Unpooled.EMPTY_BUFFER;

      final Frame frame = RECYCLER.get();
      frame.content =
          ByteBufAllocator.DEFAULT.buffer(
              SetupFrameFlyweight.computeFrameLength(
                  flags,
                  metadataMimeType,
                  dataMimeType,
                  metadata.readableBytes(),
                  data.readableBytes()));
      frame.content.writerIndex(
          SetupFrameFlyweight.encode(
              frame.content,
              flags,
              keepaliveInterval,
              maxLifetime,
              metadataMimeType,
              dataMimeType,
              metadata,
              data));
      return frame;
    }

    public static int getFlags(final Frame frame) {
      ensureFrameType(FrameType.SETUP, frame);
      final int flags = FrameHeaderFlyweight.flags(frame.content);

      return flags & SetupFrameFlyweight.VALID_FLAGS;
    }

    public static int version(final Frame frame) {
      ensureFrameType(FrameType.SETUP, frame);
      return SetupFrameFlyweight.version(frame.content);
    }

    public static int keepaliveInterval(final Frame frame) {
      ensureFrameType(FrameType.SETUP, frame);
      return SetupFrameFlyweight.keepaliveInterval(frame.content);
    }

    public static int maxLifetime(final Frame frame) {
      ensureFrameType(FrameType.SETUP, frame);
      return SetupFrameFlyweight.maxLifetime(frame.content);
    }

    public static String metadataMimeType(final Frame frame) {
      ensureFrameType(FrameType.SETUP, frame);
      return SetupFrameFlyweight.metadataMimeType(frame.content);
    }

    public static String dataMimeType(final Frame frame) {
      ensureFrameType(FrameType.SETUP, frame);
      return SetupFrameFlyweight.dataMimeType(frame.content);
    }
  }

  public static class Error {
    private static final Logger errorLogger = LoggerFactory.getLogger(Error.class);

    private Error() {}

    public static Frame from(int streamId, final Throwable throwable, ByteBuf dataBuffer) {
      if (errorLogger.isDebugEnabled()) {
        errorLogger.debug("an error occurred, creating error frame", throwable);
      }

      final int code = ErrorFrameFlyweight.errorCodeFromException(throwable);
      final Frame frame = RECYCLER.get();
      frame.content =
          ByteBufAllocator.DEFAULT.buffer(
              ErrorFrameFlyweight.computeFrameLength(dataBuffer.readableBytes()));
      frame.content.writerIndex(
          ErrorFrameFlyweight.encode(frame.content, streamId, code, dataBuffer));
      return frame;
    }

    public static Frame from(int streamId, final Throwable throwable) {
      String data = throwable.getMessage() == null ? "" : throwable.getMessage();
      byte[] bytes = data.getBytes(StandardCharsets.UTF_8);

      return from(streamId, throwable, Unpooled.wrappedBuffer(bytes));
    }

    public static int errorCode(final Frame frame) {
      ensureFrameType(FrameType.ERROR, frame);
      return ErrorFrameFlyweight.errorCode(frame.content);
    }

    public static String message(Frame frame) {
      ensureFrameType(FrameType.ERROR, frame);
      return ErrorFrameFlyweight.message(frame.content);
    }
  }

  public static class Lease {
    private Lease() {}

    public static Frame from(int ttl, int numberOfRequests, ByteBuf metadata) {
      final Frame frame = RECYCLER.get();
      frame.content =
          ByteBufAllocator.DEFAULT.buffer(
              LeaseFrameFlyweight.computeFrameLength(metadata.readableBytes()));
      frame.content.writerIndex(
          LeaseFrameFlyweight.encode(frame.content, ttl, numberOfRequests, metadata));
      return frame;
    }

    public static int ttl(final Frame frame) {
      ensureFrameType(FrameType.LEASE, frame);
      return LeaseFrameFlyweight.ttl(frame.content);
    }

    public static int numberOfRequests(final Frame frame) {
      ensureFrameType(FrameType.LEASE, frame);
      return LeaseFrameFlyweight.numRequests(frame.content);
    }
  }

  public static class RequestN {
    private RequestN() {}

    public static Frame from(int streamId, long requestN) {
      int v = requestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) requestN;
      return from(streamId, v);
    }

    public static Frame from(int streamId, int requestN) {
      if (requestN < 1) {
        throw new IllegalStateException("request n must be greater than 0");
      }

      final Frame frame = RECYCLER.get();
      frame.content = ByteBufAllocator.DEFAULT.buffer(RequestNFrameFlyweight.computeFrameLength());
      frame.content.writerIndex(RequestNFrameFlyweight.encode(frame.content, streamId, requestN));
      return frame;
    }

    public static int requestN(final Frame frame) {
      ensureFrameType(FrameType.REQUEST_N, frame);
      return RequestNFrameFlyweight.requestN(frame.content);
    }
  }

  public static class Request {
    private Request() {}

    public static Frame from(int streamId, FrameType type, Payload payload, long initialRequestN) {
      int v = initialRequestN > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) initialRequestN;
      return from(streamId, type, payload, v);
    }

    public static Frame from(int streamId, FrameType type, Payload payload, int initialRequestN) {
      if (initialRequestN < 1) {
        throw new IllegalStateException("initial request n must be greater than 0");
      }
      final @Nullable ByteBuf metadata =
          payload.hasMetadata() ? Unpooled.wrappedBuffer(payload.getMetadata()) : null;
      final ByteBuf data = Unpooled.wrappedBuffer(payload.getData());

      final Frame frame = RECYCLER.get();
      frame.content =
          ByteBufAllocator.DEFAULT.buffer(
              RequestFrameFlyweight.computeFrameLength(
                  type, metadata != null ? metadata.readableBytes() : null, data.readableBytes()));

      if (type.hasInitialRequestN()) {
        frame.content.writerIndex(
            RequestFrameFlyweight.encode(
                frame.content,
                streamId,
                metadata != null ? FLAGS_M : 0,
                type,
                initialRequestN,
                metadata,
                data));
      } else {
        frame.content.writerIndex(
            RequestFrameFlyweight.encode(
                frame.content, streamId, metadata != null ? FLAGS_M : 0, type, metadata, data));
      }

      return frame;
    }

    public static Frame from(int streamId, FrameType type, int flags) {
      final Frame frame = RECYCLER.get();
      frame.content =
          ByteBufAllocator.DEFAULT.buffer(RequestFrameFlyweight.computeFrameLength(type, null, 0));
      frame.content.writerIndex(
          RequestFrameFlyweight.encode(
              frame.content, streamId, flags, type, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER));
      return frame;
    }

    public static Frame from(
        int streamId,
        FrameType type,
        ByteBuf metadata,
        ByteBuf data,
        int initialRequestN,
        int flags) {
      final Frame frame = RECYCLER.get();
      frame.content =
          ByteBufAllocator.DEFAULT.buffer(
              RequestFrameFlyweight.computeFrameLength(
                  type, metadata.readableBytes(), data.readableBytes()));
      frame.content.writerIndex(
          RequestFrameFlyweight.encode(
              frame.content, streamId, flags, type, initialRequestN, metadata, data));
      return frame;
    }

    public static int initialRequestN(final Frame frame) {
      final FrameType type = frame.getType();
      int result;

      if (!type.isRequestType()) {
        throw new AssertionError("expected request type, but saw " + type.name());
      }

      switch (frame.getType()) {
        case REQUEST_RESPONSE:
          result = 1;
          break;
        case FIRE_AND_FORGET:
          result = 0;
          break;
        default:
          result = RequestFrameFlyweight.initialRequestN(frame.content);
          break;
      }

      return result;
    }

    public static boolean isRequestChannelComplete(final Frame frame) {
      ensureFrameType(FrameType.REQUEST_CHANNEL, frame);
      final int flags = FrameHeaderFlyweight.flags(frame.content);

      return (flags & FrameHeaderFlyweight.FLAGS_C) == FrameHeaderFlyweight.FLAGS_C;
    }
  }

  public static class PayloadFrame {

    private PayloadFrame() {}

    public static Frame from(int streamId, FrameType type) {
      return from(streamId, type, null, Unpooled.EMPTY_BUFFER, 0);
    }

    public static Frame from(int streamId, FrameType type, Payload payload) {
      return from(streamId, type, payload, payload.hasMetadata() ? FLAGS_M : 0);
    }

    public static Frame from(int streamId, FrameType type, Payload payload, int flags) {
      final ByteBuf metadata =
          payload.hasMetadata() ? Unpooled.wrappedBuffer(payload.getMetadata()) : null;
      final ByteBuf data = Unpooled.wrappedBuffer(payload.getData());
      return from(streamId, type, metadata, data, flags);
    }

    public static Frame from(
        int streamId, FrameType type, @Nullable ByteBuf metadata, ByteBuf data, int flags) {
      final Frame frame = RECYCLER.get();
      frame.content =
          ByteBufAllocator.DEFAULT.buffer(
              FrameHeaderFlyweight.computeFrameHeaderLength(
                  type, metadata != null ? metadata.readableBytes() : null, data.readableBytes()));
      frame.content.writerIndex(
          FrameHeaderFlyweight.encode(frame.content, streamId, flags, type, metadata, data));
      return frame;
    }
  }

  public static class Cancel {

    private Cancel() {}

    public static Frame from(int streamId) {
      final Frame frame = RECYCLER.get();
      frame.content =
          ByteBufAllocator.DEFAULT.buffer(
              FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.CANCEL, null, 0));
      frame.content.writerIndex(
          FrameHeaderFlyweight.encode(
              frame.content, streamId, 0, FrameType.CANCEL, null, Unpooled.EMPTY_BUFFER));
      return frame;
    }
  }

  public static class Keepalive {

    private Keepalive() {}

    public static Frame from(ByteBuf data, boolean respond) {
      final Frame frame = RECYCLER.get();
      frame.content =
          ByteBufAllocator.DEFAULT.buffer(
              KeepaliveFrameFlyweight.computeFrameLength(data.readableBytes()));

      final int flags = respond ? KeepaliveFrameFlyweight.FLAGS_KEEPALIVE_R : 0;
      frame.content.writerIndex(KeepaliveFrameFlyweight.encode(frame.content, flags, data));

      return frame;
    }

    public static boolean hasRespondFlag(final Frame frame) {
      ensureFrameType(FrameType.KEEPALIVE, frame);
      final int flags = FrameHeaderFlyweight.flags(frame.content);

      return (flags & KeepaliveFrameFlyweight.FLAGS_KEEPALIVE_R)
          == KeepaliveFrameFlyweight.FLAGS_KEEPALIVE_R;
    }
  }

  public static void ensureFrameType(final FrameType frameType, final Frame frame) {
    final FrameType typeInFrame = frame.getType();

    if (typeInFrame != frameType) {
      throw new AssertionError("expected " + frameType + ", but saw" + typeInFrame);
    }
  }

  @Override
  public String toString() {
    FrameType type = FrameHeaderFlyweight.frameType(content);
    StringBuilder payload = new StringBuilder();
    @Nullable ByteBuf metadata = FrameHeaderFlyweight.sliceFrameMetadata(content);

    if (metadata != null) {
      if (0 < metadata.readableBytes()) {
        payload.append(
            String.format("metadata: \"%s\" ", metadata.toString(StandardCharsets.UTF_8)));
      }
    }

    ByteBuf data = FrameHeaderFlyweight.sliceFrameData(content);
    if (0 < data.readableBytes()) {
      payload.append(String.format("data: \"%s\" ", data.toString(StandardCharsets.UTF_8)));
    }

    long streamId = FrameHeaderFlyweight.streamId(content);

    String additionalFlags = "";
    switch (type) {
      case LEASE:
        additionalFlags = " Permits: " + Lease.numberOfRequests(this) + " TTL: " + Lease.ttl(this);
        break;
      case REQUEST_N:
        additionalFlags = " RequestN: " + RequestN.requestN(this);
        break;
      case KEEPALIVE:
        additionalFlags = " Respond flag: " + Keepalive.hasRespondFlag(this);
        break;
      case REQUEST_STREAM:
      case REQUEST_CHANNEL:
        additionalFlags = " Initial Request N: " + Request.initialRequestN(this);
        break;
      case ERROR:
        additionalFlags = " Error code: " + Error.errorCode(this);
        break;
      case SETUP:
        int version = Setup.version(this);
        additionalFlags =
            " Version: "
                + VersionFlyweight.toString(version)
                + " keep-alive interval: "
                + Setup.keepaliveInterval(this)
                + " max lifetime: "
                + Setup.maxLifetime(this)
                + " metadata mime type: "
                + Setup.metadataMimeType(this)
                + " data mime type: "
                + Setup.dataMimeType(this);
        break;
    }

    return "Frame => Stream ID: "
        + streamId
        + " Type: "
        + type
        + additionalFlags
        + " Payload: "
        + payload;
  }
}
