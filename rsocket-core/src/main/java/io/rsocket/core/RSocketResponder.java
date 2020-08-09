/*
 * Copyright 2015-2018 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.lease.ResponderLeaseHandler;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Responder side of RSocket. Receives {@link ByteBuf}s from a peer's {@link RSocketRequester} */
class RSocketResponder extends RequesterResponderSupport implements RSocket {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketResponder.class);

  private static final Exception CLOSED_CHANNEL_EXCEPTION = new ClosedChannelException();

  private final RSocket requestHandler;

  private final ResponderLeaseHandler leaseHandler;
  private final Disposable leaseHandlerDisposable;

  private volatile Throwable terminationError;
  private static final AtomicReferenceFieldUpdater<RSocketResponder, Throwable> TERMINATION_ERROR =
      AtomicReferenceFieldUpdater.newUpdater(
          RSocketResponder.class, Throwable.class, "terminationError");

  RSocketResponder(
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      ResponderLeaseHandler leaseHandler,
      int mtu,
      int maxFrameLength,
      int maxInboundPayloadSize) {
    super(mtu, maxFrameLength, maxInboundPayloadSize, payloadDecoder, connection, null);

    this.requestHandler = requestHandler;

    this.leaseHandler = leaseHandler;

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    // connections
    connection.receive().subscribe(this::handleFrame, e -> {});
    leaseHandlerDisposable =
        leaseHandler.send(leaseFrame -> connection.sendFrame(0, leaseFrame, true));

    connection
        .onClose()
        .subscribe(null, this::tryTerminateOnConnectionError, this::tryTerminateOnConnectionClose);
  }

  private void handleSendProcessorError(Throwable t) {
    for (FrameHandler frameHandler : activeStreams.values()) {
      frameHandler.handleError(t);
    }
  }

  private void tryTerminateOnConnectionError(Throwable e) {
    tryTerminate(() -> e);
  }

  private void tryTerminateOnConnectionClose() {
    tryTerminate(() -> CLOSED_CHANNEL_EXCEPTION);
  }

  private void tryTerminate(Supplier<Throwable> errorSupplier) {
    if (terminationError == null) {
      Throwable e = errorSupplier.get();
      if (TERMINATION_ERROR.compareAndSet(this, null, e)) {
        cleanup();
      }
    }
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.fireAndForget(payload);
      } else {
        payload.release();
        return Mono.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.requestResponse(payload);
      } else {
        payload.release();
        return Mono.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.requestStream(payload);
      } else {
        payload.release();
        return Flux.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.requestChannel(payloads);
      } else {
        return Flux.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  private Flux<Payload> requestChannel(Payload payload, Publisher<Payload> payloads) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.requestChannel(payloads);
      } else {
        payload.release();
        return Flux.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    try {
      return requestHandler.metadataPush(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public void dispose() {
    tryTerminate(() -> new CancellationException("Disposed"));
  }

  @Override
  public boolean isDisposed() {
    return getDuplexConnection().isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return getDuplexConnection().onClose();
  }

  private void cleanup() {
    cleanUpSendingSubscriptions();

    getDuplexConnection().dispose();
    leaseHandlerDisposable.dispose();
    requestHandler.dispose();
  }

  private synchronized void cleanUpSendingSubscriptions() {
    activeStreams.values().forEach(FrameHandler::handleCancel);
    activeStreams.clear();
  }

  private void handleFrame(ByteBuf frame) {
    try {
      int streamId = FrameHeaderCodec.streamId(frame);
      FrameHandler receiver;
      FrameType frameType = FrameHeaderCodec.frameType(frame);
      switch (frameType) {
        case REQUEST_FNF:
          handleFireAndForget(streamId, frame);
          break;
        case REQUEST_RESPONSE:
          handleRequestResponse(streamId, frame);
          break;
        case REQUEST_STREAM:
          long streamInitialRequestN = RequestStreamFrameCodec.initialRequestN(frame);
          handleStream(streamId, frame, streamInitialRequestN);
          break;
        case REQUEST_CHANNEL:
          long channelInitialRequestN = RequestChannelFrameCodec.initialRequestN(frame);
          handleChannel(streamId, frame, channelInitialRequestN);
          break;
        case METADATA_PUSH:
          handleMetadataPush(metadataPush(super.getPayloadDecoder().apply(frame)));
          break;
        case CANCEL:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleCancel();
          }
          break;
        case REQUEST_N:
          receiver = super.get(streamId);
          if (receiver != null) {
            long n = RequestNFrameCodec.requestN(frame);
            receiver.handleRequestN(n);
          }
          break;
        case PAYLOAD:
          // TODO: Hook in receiving socket.
          break;
        case NEXT:
          receiver = super.get(streamId);
          if (receiver != null) {
            boolean hasFollows = FrameHeaderCodec.hasFollows(frame);
            receiver.handleNext(frame, hasFollows, false);
          }
          break;
        case COMPLETE:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleComplete();
          }
          break;
        case ERROR:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleError(io.rsocket.exceptions.Exceptions.from(streamId, frame));
          }
          break;
        case NEXT_COMPLETE:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleNext(frame, false, true);
          }
          break;
        case SETUP:
          getDuplexConnection()
              .sendFrame(
                  streamId,
                  ErrorFrameCodec.encode(
                      super.getAllocator(),
                      streamId,
                      new IllegalStateException("Setup frame received post setup.")),
                  false);
          break;
        case LEASE:
        default:
          getDuplexConnection()
              .sendFrame(
                  streamId,
                  ErrorFrameCodec.encode(
                      super.getAllocator(),
                      streamId,
                      new IllegalStateException(
                          "ServerRSocket: Unexpected frame type: " + frameType)),
                  false);
          break;
      }
    } catch (Throwable t) {
      LOGGER.error("Unexpected error during frame handling", t);
      getDuplexConnection()
          .sendFrame(
              0,
              ErrorFrameCodec.encode(
                  super.getAllocator(),
                  0,
                  new ConnectionErrorException("Unexpected error during frame handling", t)),
              false);
      this.tryTerminateOnConnectionError(t);
    }
  }

  private void handleFireAndForget(int streamId, ByteBuf frame) {
    if (FrameHeaderCodec.hasFollows(frame)) {
      FireAndForgetResponderSubscriber subscriber =
          new FireAndForgetResponderSubscriber(streamId, frame, this, this);

      this.add(streamId, subscriber);
    } else {
      fireAndForget(super.getPayloadDecoder().apply(frame))
          .subscribe(FireAndForgetResponderSubscriber.INSTANCE);
    }
  }

  private void handleRequestResponse(int streamId, ByteBuf frame) {
    if (FrameHeaderCodec.hasFollows(frame)) {
      RequestResponseResponderSubscriber subscriber =
          new RequestResponseResponderSubscriber(streamId, frame, this, this);

      this.add(streamId, subscriber);
    } else {
      RequestResponseResponderSubscriber subscriber =
          new RequestResponseResponderSubscriber(streamId, this);

      if (this.add(streamId, subscriber)) {
        this.requestResponse(super.getPayloadDecoder().apply(frame)).subscribe(subscriber);
      }
    }
  }

  private void handleStream(int streamId, ByteBuf frame, long initialRequestN) {
    if (FrameHeaderCodec.hasFollows(frame)) {
      RequestStreamResponderSubscriber subscriber =
          new RequestStreamResponderSubscriber(streamId, initialRequestN, frame, this, this);

      this.add(streamId, subscriber);
    } else {
      RequestStreamResponderSubscriber subscriber =
          new RequestStreamResponderSubscriber(streamId, initialRequestN, this);

      if (this.add(streamId, subscriber)) {
        this.requestStream(super.getPayloadDecoder().apply(frame)).subscribe(subscriber);
      }
    }
  }

  private void handleChannel(int streamId, ByteBuf frame, long initialRequestN) {
    if (FrameHeaderCodec.hasFollows(frame)) {
      RequestChannelResponderSubscriber subscriber =
          new RequestChannelResponderSubscriber(streamId, initialRequestN, frame, this, this);

      this.add(streamId, subscriber);
    } else {
      final Payload firstPayload = super.getPayloadDecoder().apply(frame);
      RequestChannelResponderSubscriber subscriber =
          new RequestChannelResponderSubscriber(streamId, initialRequestN, firstPayload, this);

      if (this.add(streamId, subscriber)) {
        this.requestChannel(firstPayload, subscriber).subscribe(subscriber);
      }
    }
  }

  private void handleMetadataPush(Mono<Void> result) {
    result.subscribe(MetadataPushResponderSubscriber.INSTANCE);
  }

  private boolean add(int streamId, FrameHandler frameHandler) {
    FrameHandler existingHandler;
    synchronized (this) {
      existingHandler = super.activeStreams.putIfAbsent(streamId, frameHandler);
    }

    if (existingHandler != null) {
      frameHandler.handleCancel();
      return false;
    }

    return true;
  }
}
