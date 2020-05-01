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
import static io.rsocket.keepalive.KeepAliveSupport.ClientKeepAliveSupport;
import static io.rsocket.keepalive.KeepAliveSupport.KeepAlive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.exceptions.Exceptions;
import io.rsocket.frame.CancelFrameFlyweight;
import io.rsocket.frame.ErrorFrameFlyweight;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.MetadataPushFrameFlyweight;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.frame.RequestChannelFrameFlyweight;
import io.rsocket.frame.RequestFireAndForgetFrameFlyweight;
import io.rsocket.frame.RequestNFrameFlyweight;
import io.rsocket.frame.RequestResponseFrameFlyweight;
import io.rsocket.frame.RequestStreamFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.SynchronizedIntObjectHashMap;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.keepalive.KeepAliveFramesAcceptor;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.keepalive.KeepAliveSupport;
import io.rsocket.lease.RequesterLeaseHandler;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

/**
 * Requester Side of a RSocket socket. Sends {@link ByteBuf}s to a {@link RSocketResponder} of peer
 */
class RSocketRequester implements RSocket {
  private static final AtomicReferenceFieldUpdater<RSocketRequester, Throwable> TERMINATION_ERROR =
      AtomicReferenceFieldUpdater.newUpdater(
          RSocketRequester.class, Throwable.class, "terminationError");
  private static final Exception CLOSED_CHANNEL_EXCEPTION = new ClosedChannelException();
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

  static {
    CLOSED_CHANNEL_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  private final DuplexConnection connection;
  private final PayloadDecoder payloadDecoder;
  private final Consumer<Throwable> errorConsumer;
  private final StreamIdSupplier streamIdSupplier;
  private final IntObjectMap<Subscription> senders;
  private final IntObjectMap<Processor<Payload, Payload>> receivers;
  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final int mtu;
  private final RequesterLeaseHandler leaseHandler;
  private final ByteBufAllocator allocator;
  private final KeepAliveFramesAcceptor keepAliveFramesAcceptor;
  private volatile Throwable terminationError;
  private final MonoProcessor<Void> onClose;

  RSocketRequester(
      DuplexConnection connection,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      StreamIdSupplier streamIdSupplier,
      int mtu,
      int keepAliveTickPeriod,
      int keepAliveAckTimeout,
      @Nullable KeepAliveHandler keepAliveHandler,
      RequesterLeaseHandler leaseHandler) {
    this.connection = connection;
    this.allocator = connection.alloc();
    this.payloadDecoder = payloadDecoder;
    this.errorConsumer = errorConsumer;
    this.streamIdSupplier = streamIdSupplier;
    this.mtu = mtu;
    this.leaseHandler = leaseHandler;
    this.senders = new SynchronizedIntObjectHashMap<>();
    this.receivers = new SynchronizedIntObjectHashMap<>();
    this.onClose = MonoProcessor.create();

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    this.sendProcessor = new UnboundedProcessor<>();

    connection
        .onClose()
        .or(onClose)
        .subscribe(null, this::tryTerminateOnConnectionError, this::tryTerminateOnConnectionClose);
    connection.send(sendProcessor).subscribe(null, this::handleSendProcessorError);

    connection.receive().subscribe(this::handleIncomingFrames, errorConsumer);

    if (keepAliveTickPeriod != 0 && keepAliveHandler != null) {
      KeepAliveSupport keepAliveSupport =
          new ClientKeepAliveSupport(this.allocator, keepAliveTickPeriod, keepAliveAckTimeout);
      this.keepAliveFramesAcceptor =
          keepAliveHandler.start(
              keepAliveSupport, sendProcessor::onNextPrioritized, this::tryTerminateOnKeepAlive);
    } else {
      keepAliveFramesAcceptor = null;
    }
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return handleFireAndForget(payload);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return handleRequestResponse(payload);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return handleRequestStream(payload);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return handleChannel(Flux.from(payloads));
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return handleMetadataPush(payload);
  }

  @Override
  public double availability() {
    return Math.min(connection.availability(), leaseHandler.availability());
  }

  @Override
  public void dispose() {
    tryTerminate(() -> new CancellationException("Disposed"));
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  private Mono<Void> handleFireAndForget(Payload payload) {
    Throwable err = checkAvailable();
    if (err != null) {
      payload.release();
      return Mono.error(err);
    }

    if (!PayloadValidationUtils.isValid(this.mtu, payload)) {
      payload.release();
      return Mono.error(new IllegalArgumentException(INVALID_PAYLOAD_ERROR_MESSAGE));
    }

    final AtomicBoolean once = new AtomicBoolean();

    return Mono.defer(
        () -> {
          if (once.getAndSet(true)) {
            return Mono.error(
                new IllegalStateException("FireAndForgetMono allows only a single subscriber"));
          }

          final int streamId = streamIdSupplier.nextStreamId(receivers);
          final ByteBuf requestFrame =
              RequestFireAndForgetFrameFlyweight.encodeReleasingPayload(
                  allocator, streamId, payload);

          sendProcessor.onNext(requestFrame);

          return Mono.empty();
        });
  }

  private Mono<Payload> handleRequestResponse(final Payload payload) {
    Throwable err = checkAvailable();
    if (err != null) {
      payload.release();
      return Mono.error(err);
    }

    if (!PayloadValidationUtils.isValid(this.mtu, payload)) {
      payload.release();
      return Mono.error(new IllegalArgumentException(INVALID_PAYLOAD_ERROR_MESSAGE));
    }

    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;
    final UnicastProcessor<Payload> receiver = UnicastProcessor.create(Queues.<Payload>one().get());
    final AtomicBoolean once = new AtomicBoolean();

    return Mono.defer(
        () -> {
          if (once.getAndSet(true)) {
            return Mono.error(
                new IllegalStateException("RequestResponseMono allows only a single subscriber"));
          }

          return receiver
              .next()
              .transform(
                  Operators.<Payload, Payload>lift(
                      (s, actual) ->
                          new RequestOperator(actual) {

                            @Override
                            void hookOnFirstRequest(long n) {
                              int streamId = streamIdSupplier.nextStreamId(receivers);
                              this.streamId = streamId;

                              ByteBuf requestResponseFrame =
                                  RequestResponseFrameFlyweight.encodeReleasingPayload(
                                      allocator, streamId, payload);

                              receivers.put(streamId, receiver);
                              sendProcessor.onNext(requestResponseFrame);
                            }

                            @Override
                            void hookOnCancel() {
                              if (receivers.remove(streamId, receiver)) {
                                sendProcessor.onNext(
                                    CancelFrameFlyweight.encode(allocator, streamId));
                              } else {
                                payload.release();
                              }
                            }

                            @Override
                            public void hookOnTerminal(SignalType signalType) {
                              receivers.remove(streamId, receiver);
                            }
                          }))
              .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER);
        });
  }

  private Flux<Payload> handleRequestStream(final Payload payload) {
    Throwable err = checkAvailable();
    if (err != null) {
      payload.release();
      return Flux.error(err);
    }

    if (!PayloadValidationUtils.isValid(this.mtu, payload)) {
      payload.release();
      return Flux.error(new IllegalArgumentException(INVALID_PAYLOAD_ERROR_MESSAGE));
    }

    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;
    final UnicastProcessor<Payload> receiver = UnicastProcessor.create();
    final AtomicBoolean once = new AtomicBoolean();

    return Flux.defer(
        () -> {
          if (once.getAndSet(true)) {
            return Flux.error(
                new IllegalStateException("RequestStreamFlux allows only a single subscriber"));
          }

          return receiver
              .transform(
                  Operators.<Payload, Payload>lift(
                      (s, actual) ->
                          new RequestOperator(actual) {

                            @Override
                            void hookOnFirstRequest(long n) {
                              int streamId = streamIdSupplier.nextStreamId(receivers);
                              this.streamId = streamId;

                              ByteBuf requestStreamFrame =
                                  RequestStreamFrameFlyweight.encodeReleasingPayload(
                                      allocator, streamId, n, payload);

                              receivers.put(streamId, receiver);

                              sendProcessor.onNext(requestStreamFrame);
                            }

                            @Override
                            void hookOnRemainingRequests(long n) {
                              if (receiver.isDisposed()) {
                                return;
                              }

                              sendProcessor.onNext(
                                  RequestNFrameFlyweight.encode(allocator, streamId, n));
                            }

                            @Override
                            void hookOnCancel() {
                              if (receivers.remove(streamId, receiver)) {
                                sendProcessor.onNext(
                                    CancelFrameFlyweight.encode(allocator, streamId));
                              } else {
                                payload.release();
                              }
                            }

                            @Override
                            void hookOnTerminal(SignalType signalType) {
                              receivers.remove(streamId);
                            }
                          }))
              .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER);
        });
  }

  private Flux<Payload> handleChannel(Flux<Payload> request) {
    Throwable err = checkAvailable();
    if (err != null) {
      return Flux.error(err);
    }

    return request
        .switchOnFirst(
            (s, flux) -> {
              Payload payload = s.get();
              if (payload != null) {
                if (!PayloadValidationUtils.isValid(mtu, payload)) {
                  payload.release();
                  final IllegalArgumentException t =
                      new IllegalArgumentException(INVALID_PAYLOAD_ERROR_MESSAGE);
                  errorConsumer.accept(t);
                  return Mono.error(t);
                }
                return handleChannel(payload, flux);
              } else {
                return flux;
              }
            },
            false)
        .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER);
  }

  private Flux<? extends Payload> handleChannel(Payload initialPayload, Flux<Payload> inboundFlux) {
    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;

    final UnicastProcessor<Payload> receiver = UnicastProcessor.create();

    return receiver.transform(
        Operators.<Payload, Payload>lift(
            (s, actual) ->
                new RequestOperator(actual) {

                  final BaseSubscriber<Payload> upstreamSubscriber =
                      new BaseSubscriber<Payload>() {

                        boolean first = true;

                        @Override
                        protected void hookOnSubscribe(Subscription subscription) {
                          // noops
                        }

                        @Override
                        protected void hookOnNext(Payload payload) {
                          if (first) {
                            // need to skip first since we have already sent it
                            // no need to release it since it was released earlier on the request
                            // establishment
                            // phase
                            first = false;
                            request(1);
                            return;
                          }
                          if (!PayloadValidationUtils.isValid(mtu, payload)) {
                            payload.release();
                            cancel();
                            final IllegalArgumentException t =
                                new IllegalArgumentException(INVALID_PAYLOAD_ERROR_MESSAGE);
                            errorConsumer.accept(t);
                            // no need to send any errors.
                            sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
                            receiver.onError(t);
                            return;
                          }
                          final ByteBuf frame =
                              PayloadFrameFlyweight.encodeNextReleasingPayload(
                                  allocator, streamId, payload);

                          sendProcessor.onNext(frame);
                        }

                        @Override
                        protected void hookOnComplete() {
                          ByteBuf frame = PayloadFrameFlyweight.encodeComplete(allocator, streamId);
                          sendProcessor.onNext(frame);
                        }

                        @Override
                        protected void hookOnError(Throwable t) {
                          ByteBuf frame = ErrorFrameFlyweight.encode(allocator, streamId, t);
                          sendProcessor.onNext(frame);
                          receiver.onError(t);
                        }

                        @Override
                        protected void hookFinally(SignalType type) {
                          senders.remove(streamId, this);
                        }
                      };

                  @Override
                  void hookOnFirstRequest(long n) {
                    final int streamId = streamIdSupplier.nextStreamId(receivers);
                    this.streamId = streamId;

                    final ByteBuf frame =
                        RequestChannelFrameFlyweight.encodeReleasingPayload(
                            allocator, streamId, false, n, initialPayload);

                    senders.put(streamId, upstreamSubscriber);
                    receivers.put(streamId, receiver);

                    inboundFlux
                        .limitRate(Queues.SMALL_BUFFER_SIZE)
                        .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER)
                        .subscribe(upstreamSubscriber);

                    sendProcessor.onNext(frame);
                  }

                  @Override
                  void hookOnRemainingRequests(long n) {
                    if (receiver.isDisposed()) {
                      return;
                    }

                    sendProcessor.onNext(RequestNFrameFlyweight.encode(allocator, streamId, n));
                  }

                  @Override
                  void hookOnCancel() {
                    senders.remove(streamId, upstreamSubscriber);
                    if (receivers.remove(streamId, receiver)) {
                      sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
                    }
                  }

                  @Override
                  void hookOnTerminal(SignalType signalType) {
                    if (signalType == SignalType.ON_ERROR) {
                      upstreamSubscriber.cancel();
                    }
                    receivers.remove(streamId, receiver);
                  }

                  @Override
                  public void cancel() {
                    upstreamSubscriber.cancel();
                    super.cancel();
                  }
                }));
  }

  private Mono<Void> handleMetadataPush(Payload payload) {
    Throwable err = this.terminationError;
    if (err != null) {
      payload.release();
      return Mono.error(err);
    }

    if (!PayloadValidationUtils.isValid(this.mtu, payload)) {
      payload.release();
      return Mono.error(new IllegalArgumentException(INVALID_PAYLOAD_ERROR_MESSAGE));
    }

    final AtomicBoolean once = new AtomicBoolean();

    return Mono.defer(
        () -> {
          if (once.getAndSet(true)) {
            return Mono.error(
                new IllegalStateException("MetadataPushMono allows only a single subscriber"));
          }

          ByteBuf metadataPushFrame =
              MetadataPushFrameFlyweight.encodeReleasingPayload(allocator, payload);

          sendProcessor.onNextPrioritized(metadataPushFrame);

          return Mono.empty();
        });
  }

  @Nullable
  private Throwable checkAvailable() {
    Throwable err = this.terminationError;
    if (err != null) {
      return err;
    }
    RequesterLeaseHandler lh = leaseHandler;
    if (!lh.useLease()) {
      return lh.leaseError();
    }
    return null;
  }

  private void handleIncomingFrames(ByteBuf frame) {
    try {
      int streamId = FrameHeaderFlyweight.streamId(frame);
      FrameType type = FrameHeaderFlyweight.frameType(frame);
      if (streamId == 0) {
        handleStreamZero(type, frame);
      } else {
        handleFrame(streamId, type, frame);
      }
      frame.release();
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(frame);
      throw reactor.core.Exceptions.propagate(t);
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
        errorConsumer.accept(
            new IllegalStateException(
                "Client received supported frame on stream 0: " + frame.toString()));
    }
  }

  private void handleFrame(int streamId, FrameType type, ByteBuf frame) {
    Subscriber<Payload> receiver = receivers.get(streamId);
    switch (type) {
      case NEXT:
        if (receiver == null) {
          handleMissingResponseProcessor(streamId, type, frame);
          return;
        }
        receiver.onNext(payloadDecoder.apply(frame));
        break;
      case NEXT_COMPLETE:
        if (receiver == null) {
          handleMissingResponseProcessor(streamId, type, frame);
          return;
        }
        receiver.onNext(payloadDecoder.apply(frame));
        receiver.onComplete();
        break;
      case COMPLETE:
        if (receiver == null) {
          handleMissingResponseProcessor(streamId, type, frame);
          return;
        }
        receiver.onComplete();
        receivers.remove(streamId);
        break;
      case ERROR:
        if (receiver == null) {
          handleMissingResponseProcessor(streamId, type, frame);
          return;
        }
        receiver.onError(Exceptions.from(streamId, frame));
        receivers.remove(streamId);
        break;
      case CANCEL:
        {
          Subscription sender = senders.remove(streamId);
          if (sender != null) {
            sender.cancel();
          }
          break;
        }
      case REQUEST_N:
        {
          Subscription sender = senders.get(streamId);
          if (sender != null) {
            long n = RequestNFrameFlyweight.requestN(frame);
            sender.request(n);
          }
          break;
        }
      default:
        throw new IllegalStateException(
            "Client received supported frame on stream " + streamId + ": " + frame.toString());
    }
  }

  private void handleMissingResponseProcessor(int streamId, FrameType type, ByteBuf frame) {
    if (!streamIdSupplier.isBeforeOrCurrent(streamId)) {
      if (type == FrameType.ERROR) {
        // message for stream that has never existed, we have a problem with
        // the overall connection and must tear down
        String errorMessage = ErrorFrameFlyweight.dataUtf8(frame);

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

  private void tryTerminateOnKeepAlive(KeepAlive keepAlive) {
    tryTerminate(
        () ->
            new ConnectionErrorException(
                String.format("No keep-alive acks for %d ms", keepAlive.getTimeout().toMillis())));
  }

  private void tryTerminateOnConnectionError(Throwable e) {
    tryTerminate(() -> e);
  }

  private void tryTerminateOnConnectionClose() {
    tryTerminate(() -> CLOSED_CHANNEL_EXCEPTION);
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

  private void terminate(Throwable e) {
    connection.dispose();
    leaseHandler.dispose();

    synchronized (receivers) {
      receivers
          .values()
          .forEach(
              receiver -> {
                try {
                  receiver.onError(e);
                } catch (Throwable t) {
                  errorConsumer.accept(t);
                }
              });
    }
    synchronized (senders) {
      senders
          .values()
          .forEach(
              sender -> {
                try {
                  sender.cancel();
                } catch (Throwable t) {
                  errorConsumer.accept(t);
                }
              });
    }
    senders.clear();
    receivers.clear();
    sendProcessor.dispose();
    errorConsumer.accept(e);
    onClose.onError(e);
  }

  private void handleSendProcessorError(Throwable t) {
    connection.dispose();
  }
}
