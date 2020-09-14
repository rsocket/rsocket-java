/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.core;

import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;
import static io.rsocket.core.PayloadValidationUtils.isValidMetadata;
import static io.rsocket.core.StateUtils.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.frame.MetadataPushFrameCodec;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

final class MetadataPushRequesterMono extends Mono<Void> implements Scannable {

  volatile long state;
  static final AtomicLongFieldUpdater<MetadataPushRequesterMono> STATE =
      AtomicLongFieldUpdater.newUpdater(MetadataPushRequesterMono.class, "state");

  final ByteBufAllocator allocator;
  final Payload payload;
  final int maxFrameLength;
  final DuplexConnection connection;

  MetadataPushRequesterMono(Payload payload, RequesterResponderSupport requesterResponderSupport) {
    this.allocator = requesterResponderSupport.getAllocator();
    this.payload = payload;
    this.maxFrameLength = requesterResponderSupport.getMaxFrameLength();
    this.connection = requesterResponderSupport.getDuplexConnection();
  }

  @Override
  public void subscribe(CoreSubscriber<? super Void> actual) {
    long previousState = markSubscribed(STATE, this);
    if (isSubscribedOrTerminated(previousState)) {
      Operators.error(
          actual, new IllegalStateException("MetadataPushMono allows only a single Subscriber"));
      return;
    }

    final Payload p = this.payload;
    final ByteBuf metadata;
    try {
      final boolean hasMetadata = p.hasMetadata();
      metadata = p.metadata();
      if (!hasMetadata) {
        lazyTerminate(STATE, this);
        p.release();
        Operators.error(
            actual,
            new IllegalArgumentException("Metadata push should have metadata field present"));
        return;
      }
      if (!isValidMetadata(this.maxFrameLength, metadata)) {
        lazyTerminate(STATE, this);
        p.release();
        Operators.error(
            actual,
            new IllegalArgumentException(
                String.format(INVALID_PAYLOAD_ERROR_MESSAGE, this.maxFrameLength)));
        return;
      }
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);
      Operators.error(actual, e);
      return;
    }

    final ByteBuf metadataRetainedSlice;
    try {
      metadataRetainedSlice = metadata.retainedSlice();
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);
      Operators.error(actual, e);
      return;
    }

    try {
      p.release();
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);
      metadataRetainedSlice.release();
      Operators.error(actual, e);
      return;
    }

    final ByteBuf requestFrame =
        MetadataPushFrameCodec.encode(this.allocator, metadataRetainedSlice);
    this.connection.sendFrame(0, requestFrame);

    Operators.complete(actual);
  }

  @Override
  @Nullable
  public Void block(Duration m) {
    return block();
  }

  @Override
  @Nullable
  public Void block() {
    long previousState = markSubscribed(STATE, this);
    if (isSubscribedOrTerminated(previousState)) {
      throw new IllegalStateException("MetadataPushMono allows only a single Subscriber");
    }

    final Payload p = this.payload;
    final ByteBuf metadata;
    try {
      final boolean hasMetadata = p.hasMetadata();
      metadata = p.metadata();
      if (hasMetadata) {
        lazyTerminate(STATE, this);
        p.release();
        throw new IllegalArgumentException("Metadata push does not support metadata field");
      }
      if (!isValidMetadata(this.maxFrameLength, metadata)) {
        lazyTerminate(STATE, this);
        p.release();
        throw new IllegalArgumentException("Too Big Payload size");
      }
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);
      throw e;
    }

    final ByteBuf metadataRetainedSlice;
    try {
      metadataRetainedSlice = metadata.retainedSlice();
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);
      throw e;
    }

    try {
      p.release();
    } catch (IllegalReferenceCountException e) {
      lazyTerminate(STATE, this);
      metadataRetainedSlice.release();
      throw e;
    }

    final ByteBuf requestFrame =
        MetadataPushFrameCodec.encode(this.allocator, metadataRetainedSlice);
    this.connection.sendFrame(0, requestFrame);

    return null;
  }

  @Override
  public Object scanUnsafe(Attr key) {
    return null; // no particular key to be represented, still useful in hooks
  }

  @Override
  @NonNull
  public String stepName() {
    return "source(MetadataPushMono)";
  }
}
