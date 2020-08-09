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

import static io.rsocket.keepalive.KeepAliveSupport.ClientKeepAliveSupport;

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.exceptions.Exceptions;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.keepalive.KeepAliveFramesAcceptor;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.keepalive.KeepAliveSupport;
import io.rsocket.lease.RequesterLeaseHandler;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.annotation.Nullable;

/**
 * Requester Side of a RSocket socket. Sends {@link ByteBuf}s to a {@link RSocketResponder} of peer
 */
class RSocketRequester extends RequesterResponderSupport implements RSocket {
  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketRequester.class);

  private static final Exception CLOSED_CHANNEL_EXCEPTION = new ClosedChannelException();

  static {
    CLOSED_CHANNEL_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  private volatile Throwable terminationError;
  private static final AtomicReferenceFieldUpdater<RSocketRequester, Throwable> TERMINATION_ERROR =
      AtomicReferenceFieldUpdater.newUpdater(
          RSocketRequester.class, Throwable.class, "terminationError");

  private final RequesterLeaseHandler leaseHandler;
  private final KeepAliveFramesAcceptor keepAliveFramesAcceptor;
  private final MonoProcessor<Void> onClose;

  RSocketRequester(
      DuplexConnection connection,
      PayloadDecoder payloadDecoder,
      StreamIdSupplier streamIdSupplier,
      int mtu,
      int maxFrameLength,
      int maxInboundPayloadSize,
      int keepAliveTickPeriod,
      int keepAliveAckTimeout,
      @Nullable KeepAliveHandler keepAliveHandler,
      RequesterLeaseHandler leaseHandler) {
    super(mtu, maxFrameLength, maxInboundPayloadSize, payloadDecoder, connection, streamIdSupplier);

    this.leaseHandler = leaseHandler;
    this.onClose = MonoProcessor.create();

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    connection.onClose().subscribe(null, this::tryTerminateOnConnectionError, this::tryShutdown);

    connection.receive().subscribe(this::handleIncomingFrames, e -> {});

    if (keepAliveTickPeriod != 0 && keepAliveHandler != null) {
      KeepAliveSupport keepAliveSupport =
          new ClientKeepAliveSupport(this.getAllocator(), keepAliveTickPeriod, keepAliveAckTimeout);
      this.keepAliveFramesAcceptor =
          keepAliveHandler.start(
              keepAliveSupport,
              (keepAliveFrame) -> connection.sendFrame(0, keepAliveFrame, true),
              this::tryTerminateOnKeepAlive);
    } else {
      keepAliveFramesAcceptor = null;
    }
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return new FireAndForgetRequesterMono(payload, this);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return new RequestResponseRequesterMono(payload, this);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return new RequestStreamRequesterFlux(payload, this);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new RequestChannelRequesterFlux(payloads, this);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    Throwable terminationError = this.terminationError;
    if (terminationError != null) {
      payload.release();
      return Mono.error(terminationError);
    }

    return new MetadataPushRequesterMono(payload, this);
  }

  @Override
  public int getNextStreamId() {
    RequesterLeaseHandler leaseHandler = this.leaseHandler;
    if (!leaseHandler.useLease()) {
      throw reactor.core.Exceptions.propagate(leaseHandler.leaseError());
    }

    int nextStreamId = super.getNextStreamId();

    Throwable terminationError = this.terminationError;
    if (terminationError != null) {
      throw reactor.core.Exceptions.propagate(terminationError);
    }

    return nextStreamId;
  }

  @Override
  public int addAndGetNextStreamId(FrameHandler frameHandler) {
    RequesterLeaseHandler leaseHandler = this.leaseHandler;
    if (!leaseHandler.useLease()) {
      throw reactor.core.Exceptions.propagate(leaseHandler.leaseError());
    }

    int nextStreamId = super.addAndGetNextStreamId(frameHandler);

    Throwable terminationError = this.terminationError;
    if (terminationError != null) {
      super.remove(nextStreamId, frameHandler);
      throw reactor.core.Exceptions.propagate(terminationError);
    }

    return nextStreamId;
  }

  @Override
  public double availability() {
    return Math.min(getDuplexConnection().availability(), leaseHandler.availability());
  }

  @Override
  public void dispose() {
    tryShutdown();
  }

  @Override
  public boolean isDisposed() {
    return terminationError != null;
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  private void handleIncomingFrames(ByteBuf frame) {
    try {
      int streamId = FrameHeaderCodec.streamId(frame);
      FrameType type = FrameHeaderCodec.frameType(frame);
      if (streamId == 0) {
        handleStreamZero(type, frame);
      } else {
        handleFrame(streamId, type, frame);
      }
    } catch (Throwable t) {
      LOGGER.error("Unexpected error during frame handling", t);
      final ConnectionErrorException error =
          new ConnectionErrorException("Unexpected error during frame handling", t);
      getDuplexConnection()
          .terminate(ErrorFrameCodec.encode(super.getAllocator(), 0, error), error);
    }
  }

  private void handleStreamZero(FrameType type, ByteBuf frame) {
    switch (type) {
      case ERROR:
        tryTerminateOnZeroError(frame);
        break;
      case LEASE:
        leaseHandler.receive(frame);
        break;
      case KEEPALIVE:
        if (keepAliveFramesAcceptor != null) {
          keepAliveFramesAcceptor.receive(frame);
        }
        break;
      default:
        // Ignore unknown frames. Throwing an error will close the socket.
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("Requester received unsupported frame on stream 0: " + frame.toString());
        }
    }
  }

  private void handleFrame(int streamId, FrameType type, ByteBuf frame) {
    FrameHandler receiver = this.get(streamId);
    if (receiver == null) {
      handleMissingResponseProcessor(streamId, type, frame);
      return;
    }

    switch (type) {
      case NEXT_COMPLETE:
        receiver.handleNext(frame, false, true);
        break;
      case NEXT:
        boolean hasFollows = FrameHeaderCodec.hasFollows(frame);
        receiver.handleNext(frame, hasFollows, false);
        break;
      case COMPLETE:
        receiver.handleComplete();
        break;
      case ERROR:
        receiver.handleError(Exceptions.from(streamId, frame));
        break;
      case CANCEL:
        receiver.handleCancel();
        break;
      case REQUEST_N:
        long n = RequestNFrameCodec.requestN(frame);
        receiver.handleRequestN(n);
        break;
      default:
        throw new IllegalStateException(
            "Requester received unsupported frame on stream " + streamId + ": " + frame.toString());
    }
  }

  @SuppressWarnings("ConstantConditions")
  private void handleMissingResponseProcessor(int streamId, FrameType type, ByteBuf frame) {
    if (!super.streamIdSupplier.isBeforeOrCurrent(streamId)) {
      if (type == FrameType.ERROR) {
        // message for stream that has never existed, we have a problem with
        // the overall connection and must tear down
        String errorMessage = ErrorFrameCodec.dataUtf8(frame);

        throw new IllegalStateException(
            "Client received error for non-existent stream: "
                + streamId
                + " Message: "
                + errorMessage);
      } else {
        throw new IllegalStateException(
            "Client received message for non-existent stream: "
                + streamId
                + ", frame type: "
                + type);
      }
    }
    // receiving a frame after a given stream has been cancelled/completed,
    // so ignore (cancellation is async so there is a race condition)
  }

  private void tryTerminateOnKeepAlive(KeepAliveSupport.KeepAlive keepAlive) {
    tryTerminate(
        () ->
            new ConnectionErrorException(
                String.format("No keep-alive acks for %d ms", keepAlive.getTimeout().toMillis())));
  }

  private void tryTerminateOnConnectionError(Throwable e) {
    tryTerminate(() -> e);
  }

  private void tryTerminateOnZeroError(ByteBuf errorFrame) {
    tryTerminate(() -> Exceptions.from(0, errorFrame));
  }

  private void tryTerminate(Supplier<Throwable> errorSupplier) {
    if (terminationError == null) {
      Throwable e = errorSupplier.get();
      if (TERMINATION_ERROR.compareAndSet(this, null, e)) {
        terminate(e);
      }
    }
  }

  private void tryShutdown() {
    if (terminationError == null) {
      if (TERMINATION_ERROR.compareAndSet(this, null, CLOSED_CHANNEL_EXCEPTION)) {
        terminate(CLOSED_CHANNEL_EXCEPTION);
      }
    }
  }

  private void terminate(Throwable e) {
    if (keepAliveFramesAcceptor != null) {
      keepAliveFramesAcceptor.dispose();
    }
    getDuplexConnection().dispose();
    leaseHandler.dispose();

    synchronized (this) {
      activeStreams
          .values()
          .forEach(
              receiver -> {
                try {
                  receiver.handleError(e);
                } catch (Throwable ignored) {
                }
              });
    }

    if (e == CLOSED_CHANNEL_EXCEPTION) {
      onClose.onComplete();
    } else {
      onClose.onError(e);
    }
  }
}
