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

import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.frame.*;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.SynchronizedIntObjectHashMap;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.lease.ResponderLeaseHandler;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.*;
import reactor.util.annotation.Nullable;

/** Responder side of RSocket. Receives {@link ByteBuf}s from a peer's {@link RSocketRequester} */
class RSocketResponder implements RSocket {
  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketResponder.class);

  private static final Consumer<ReferenceCounted> DROPPED_ELEMENTS_CONSUMER =
      referenceCounted -> {
        if (referenceCounted.refCnt() > 0) {
          try {
            referenceCounted.release();
          } catch (IllegalReferenceCountException e) {
            // ignored
          }
        }
      };
  private static final Exception CLOSED_CHANNEL_EXCEPTION = new ClosedChannelException();

  private final DuplexConnection connection;
  private final RSocket requestHandler;

  @SuppressWarnings("deprecation")
  private final io.rsocket.ResponderRSocket responderRSocket;

  private final PayloadDecoder payloadDecoder;
  private final ResponderLeaseHandler leaseHandler;
  private final Disposable leaseHandlerDisposable;

  private volatile Throwable terminationError;
  private static final AtomicReferenceFieldUpdater<RSocketResponder, Throwable> TERMINATION_ERROR =
      AtomicReferenceFieldUpdater.newUpdater(
          RSocketResponder.class, Throwable.class, "terminationError");

  private final int mtu;

  private final IntObjectMap<Subscription> sendingSubscriptions;
  private final IntObjectMap<Processor<Payload, Payload>> channelProcessors;

  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final ByteBufAllocator allocator;

  RSocketResponder(
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      ResponderLeaseHandler leaseHandler,
      int mtu) {
    this.connection = connection;
    this.allocator = connection.alloc();
    this.mtu = mtu;

    this.requestHandler = requestHandler;
    this.responderRSocket =
        (requestHandler instanceof io.rsocket.ResponderRSocket)
            ? (io.rsocket.ResponderRSocket) requestHandler
            : null;

    this.payloadDecoder = payloadDecoder;
    this.leaseHandler = leaseHandler;
    this.sendingSubscriptions = new SynchronizedIntObjectHashMap<>();
    this.channelProcessors = new SynchronizedIntObjectHashMap<>();

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    // connections
    this.sendProcessor = new UnboundedProcessor<>();

    connection.send(sendProcessor).subscribe(null, this::handleSendProcessorError);

    connection.receive().subscribe(this::handleFrame, e -> {});
    leaseHandlerDisposable = leaseHandler.send(sendProcessor::onNextPrioritized);

    this.connection
        .onClose()
        .subscribe(null, this::tryTerminateOnConnectionError, this::tryTerminateOnConnectionClose);
  }

  private void handleSendProcessorError(Throwable t) {
    sendingSubscriptions
        .values()
        .forEach(
            subscription -> {
              try {
                subscription.cancel();
              } catch (Throwable e) {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("Dropped exception", t);
                }
              }
            });

    channelProcessors
        .values()
        .forEach(
            subscription -> {
              try {
                subscription.onError(t);
              } catch (Throwable e) {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("Dropped exception", t);
                }
              }
            });
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
        cleanup(e);
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
        return responderRSocket.requestChannel(payload, payloads);
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
    return connection.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return connection.onClose();
  }

  private void cleanup(Throwable e) {
    cleanUpSendingSubscriptions();
    cleanUpChannelProcessors(e);

    connection.dispose();
    leaseHandlerDisposable.dispose();
    requestHandler.dispose();
    sendProcessor.dispose();
  }

  private synchronized void cleanUpSendingSubscriptions() {
    sendingSubscriptions.values().forEach(Subscription::cancel);
    sendingSubscriptions.clear();
  }

  private synchronized void cleanUpChannelProcessors(Throwable e) {
    channelProcessors
        .values()
        .forEach(
            payloadPayloadProcessor -> {
              try {
                payloadPayloadProcessor.onError(e);
              } catch (Throwable t) {
                // noops
              }
            });
    channelProcessors.clear();
  }

  private void handleFrame(ByteBuf frame) {
    try {
      int streamId = FrameHeaderCodec.streamId(frame);
      Subscriber<Payload> receiver;
      FrameType frameType = FrameHeaderCodec.frameType(frame);
      switch (frameType) {
        case REQUEST_FNF:
          handleFireAndForget(streamId, fireAndForget(payloadDecoder.apply(frame)));
          break;
        case REQUEST_RESPONSE:
          handleRequestResponse(streamId, requestResponse(payloadDecoder.apply(frame)));
          break;
        case CANCEL:
          handleCancelFrame(streamId);
          break;
        case REQUEST_N:
          handleRequestN(streamId, frame);
          break;
        case REQUEST_STREAM:
          long streamInitialRequestN = RequestStreamFrameCodec.initialRequestN(frame);
          Payload streamPayload = payloadDecoder.apply(frame);
          handleStream(streamId, requestStream(streamPayload), streamInitialRequestN, null);
          break;
        case REQUEST_CHANNEL:
          long channelInitialRequestN = RequestChannelFrameCodec.initialRequestN(frame);
          Payload channelPayload = payloadDecoder.apply(frame);
          handleChannel(streamId, channelPayload, channelInitialRequestN);
          break;
        case METADATA_PUSH:
          handleMetadataPush(metadataPush(payloadDecoder.apply(frame)));
          break;
        case PAYLOAD:
          // TODO: Hook in receiving socket.
          break;
        case NEXT:
          receiver = channelProcessors.get(streamId);
          if (receiver != null) {
            receiver.onNext(payloadDecoder.apply(frame));
          }
          break;
        case COMPLETE:
          receiver = channelProcessors.get(streamId);
          if (receiver != null) {
            receiver.onComplete();
          }
          break;
        case ERROR:
          receiver = channelProcessors.get(streamId);
          if (receiver != null) {
            receiver.onError(new ApplicationErrorException(ErrorFrameCodec.dataUtf8(frame)));
          }
          break;
        case NEXT_COMPLETE:
          receiver = channelProcessors.get(streamId);
          if (receiver != null) {
            receiver.onNext(payloadDecoder.apply(frame));
            receiver.onComplete();
          }
          break;
        case SETUP:
          handleError(streamId, new IllegalStateException("Setup frame received post setup."));
          break;
        case LEASE:
        default:
          handleError(
              streamId,
              new IllegalStateException("ServerRSocket: Unexpected frame type: " + frameType));
          break;
      }
      ReferenceCountUtil.safeRelease(frame);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(frame);
      throw Exceptions.propagate(t);
    }
  }

  private void handleFireAndForget(int streamId, Mono<Void> result) {
    result.subscribe(
        new BaseSubscriber<Void>() {
          @Override
          protected void hookOnSubscribe(Subscription subscription) {
            sendingSubscriptions.put(streamId, subscription);
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          protected void hookOnError(Throwable throwable) {}

          @Override
          protected void hookFinally(SignalType type) {
            sendingSubscriptions.remove(streamId);
          }
        });
  }

  private void handleRequestResponse(int streamId, Mono<Payload> response) {
    final BaseSubscriber<Payload> subscriber =
        new BaseSubscriber<Payload>() {
          private boolean isEmpty = true;

          @Override
          protected void hookOnNext(Payload payload) {
            if (isEmpty) {
              isEmpty = false;
            }

            if (!PayloadValidationUtils.isValid(mtu, payload)) {
              payload.release();
              cancel();
              final IllegalArgumentException t =
                  new IllegalArgumentException(INVALID_PAYLOAD_ERROR_MESSAGE);
              handleError(streamId, t);
              return;
            }

            ByteBuf byteBuf =
                PayloadFrameCodec.encodeNextCompleteReleasingPayload(allocator, streamId, payload);
            sendProcessor.onNext(byteBuf);
          }

          @Override
          protected void hookOnError(Throwable throwable) {
            handleError(streamId, throwable);
          }

          @Override
          protected void hookOnComplete() {
            if (isEmpty) {
              sendProcessor.onNext(PayloadFrameCodec.encodeComplete(allocator, streamId));
            }
          }

          @Override
          protected void hookFinally(SignalType type) {
            sendingSubscriptions.remove(streamId, this);
          }
        };

    sendingSubscriptions.put(streamId, subscriber);
    response.doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER).subscribe(subscriber);
  }

  private void handleStream(
      int streamId,
      Flux<Payload> response,
      long initialRequestN,
      @Nullable UnicastProcessor<Payload> requestChannel) {
    final BaseSubscriber<Payload> subscriber =
        new BaseSubscriber<Payload>() {

          @Override
          protected void hookOnSubscribe(Subscription s) {
            s.request(initialRequestN);
          }

          @Override
          protected void hookOnNext(Payload payload) {
            try {
              if (!PayloadValidationUtils.isValid(mtu, payload)) {
                payload.release();
                // specifically for requestChannel case so when Payload is invalid we will not be
                // sending CancelFrame and ErrorFrame
                // Note: CancelFrame is redundant and due to spec
                // (https://github.com/rsocket/rsocket/blob/master/Protocol.md#request-channel)
                // Upon receiving an ERROR[APPLICATION_ERROR|REJECTED|CANCELED|INVALID], the stream
                // is
                // terminated on both Requester and Responder.
                // Upon sending an ERROR[APPLICATION_ERROR|REJECTED|CANCELED|INVALID], the stream is
                // terminated on both the Requester and Responder.
                if (requestChannel != null) {
                  channelProcessors.remove(streamId, requestChannel);
                }
                cancel();
                final IllegalArgumentException t =
                    new IllegalArgumentException(INVALID_PAYLOAD_ERROR_MESSAGE);
                handleError(streamId, t);
                return;
              }

              ByteBuf byteBuf =
                  PayloadFrameCodec.encodeNextReleasingPayload(allocator, streamId, payload);
              sendProcessor.onNext(byteBuf);
            } catch (Throwable e) {
              // specifically for requestChannel case so when Payload is invalid we will not be
              // sending CancelFrame and ErrorFrame
              // Note: CancelFrame is redundant and due to spec
              // (https://github.com/rsocket/rsocket/blob/master/Protocol.md#request-channel)
              // Upon receiving an ERROR[APPLICATION_ERROR|REJECTED|CANCELED|INVALID], the stream is
              // terminated on both Requester and Responder.
              // Upon sending an ERROR[APPLICATION_ERROR|REJECTED|CANCELED|INVALID], the stream is
              // terminated on both the Requester and Responder.
              if (requestChannel != null) {
                channelProcessors.remove(streamId, requestChannel);
              }
              cancel();
              handleError(streamId, e);
            }
          }

          @Override
          protected void hookOnComplete() {
            sendProcessor.onNext(PayloadFrameCodec.encodeComplete(allocator, streamId));
          }

          @Override
          protected void hookOnError(Throwable throwable) {
            handleError(streamId, throwable);
          }

          @Override
          protected void hookOnCancel() {
            // specifically for requestChannel case so when requester sends Cancel frame so the
            // whole chain MUST be terminated
            // Note: CancelFrame is redundant from the responder side due to spec
            // (https://github.com/rsocket/rsocket/blob/master/Protocol.md#request-channel)
            // Upon receiving a CANCEL, the stream is terminated on the Responder.
            // Upon sending a CANCEL, the stream is terminated on the Requester.
            if (requestChannel != null) {
              channelProcessors.remove(streamId, requestChannel);
              try {
                requestChannel.dispose();
              } catch (Exception e) {
                // might be thrown back if stream is cancelled
              }
            }
          }

          @Override
          protected void hookFinally(SignalType type) {
            sendingSubscriptions.remove(streamId);
          }
        };

    sendingSubscriptions.put(streamId, subscriber);
    response.doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER).subscribe(subscriber);
  }

  private void handleChannel(int streamId, Payload payload, long initialRequestN) {
    UnicastProcessor<Payload> frames = UnicastProcessor.create();
    channelProcessors.put(streamId, frames);

    Flux<Payload> payloads =
        frames
            .doOnRequest(
                new LongConsumer() {
                  boolean first = true;

                  @Override
                  public void accept(long l) {
                    long n;
                    if (first) {
                      first = false;
                      n = l - 1L;
                    } else {
                      n = l;
                    }
                    if (n > 0) {
                      sendProcessor.onNext(RequestNFrameCodec.encode(allocator, streamId, n));
                    }
                  }
                })
            .doFinally(
                signalType -> {
                  if (channelProcessors.remove(streamId, frames)) {
                    if (signalType == SignalType.CANCEL) {
                      sendProcessor.onNext(CancelFrameCodec.encode(allocator, streamId));
                    }
                  }
                })
            .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER);

    // not chained, as the payload should be enqueued in the Unicast processor before this method
    // returns
    // and any later payload can be processed
    frames.onNext(payload);

    if (responderRSocket != null) {
      handleStream(streamId, requestChannel(payload, payloads), initialRequestN, frames);
    } else {
      handleStream(streamId, requestChannel(payloads), initialRequestN, frames);
    }
  }

  private void handleMetadataPush(Mono<Void> result) {
    result.subscribe(
        new BaseSubscriber<Void>() {
          @Override
          protected void hookOnSubscribe(Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          protected void hookOnError(Throwable throwable) {}
        });
  }

  private void handleCancelFrame(int streamId) {
    Subscription subscription = sendingSubscriptions.remove(streamId);
    channelProcessors.remove(streamId);

    if (subscription != null) {
      subscription.cancel();
    }
  }

  private void handleError(int streamId, Throwable t) {
    sendProcessor.onNext(ErrorFrameCodec.encode(allocator, streamId, t));
  }

  private void handleRequestN(int streamId, ByteBuf frame) {
    Subscription subscription = sendingSubscriptions.get(streamId);

    if (subscription != null) {
      long n = RequestNFrameCodec.requestN(frame);
      subscription.request(n);
    }
  }
}
