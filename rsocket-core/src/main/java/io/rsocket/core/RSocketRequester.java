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

import static io.rsocket.core.LimitRateOperator.applyLimitRateOperator;
import static io.rsocket.core.PayloadValidationUtils.INVALID_PAYLOAD_ERROR_MESSAGE;
import static io.rsocket.keepalive.KeepAliveSupport.ClientKeepAliveSupport;
import static io.rsocket.keepalive.KeepAliveSupport.KeepAlive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
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
import io.rsocket.internal.UnicastMonoEmpty;
import io.rsocket.internal.UnicastMonoProcessor;
import io.rsocket.keepalive.KeepAliveFramesAcceptor;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.keepalive.KeepAliveSupport;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.util.MonoLifecycleHandler;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
      ReferenceCountUtil::safeRelease;

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

  RSocketRequester(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      StreamIdSupplier streamIdSupplier,
      int mtu,
      int keepAliveTickPeriod,
      int keepAliveAckTimeout,
      @Nullable KeepAliveHandler keepAliveHandler,
      RequesterLeaseHandler leaseHandler) {
    this.allocator = allocator;
    this.connection = connection;
    this.payloadDecoder = payloadDecoder;
    this.errorConsumer = errorConsumer;
    this.streamIdSupplier = streamIdSupplier;
    this.mtu = mtu;
    this.leaseHandler = leaseHandler;
    this.senders = new SynchronizedIntObjectHashMap<>();
    this.receivers = new SynchronizedIntObjectHashMap<>();

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    this.sendProcessor = new UnboundedProcessor<>();

    connection
        .onClose()
        .doFinally(signalType -> tryTerminateOnConnectionClose())
        .subscribe(null, errorConsumer);
    connection.send(sendProcessor).subscribe(null, this::handleSendProcessorError);

    connection.receive().subscribe(this::handleIncomingFrames, errorConsumer);

    if (keepAliveTickPeriod != 0 && keepAliveHandler != null) {
      KeepAliveSupport keepAliveSupport =
          new ClientKeepAliveSupport(allocator, keepAliveTickPeriod, keepAliveAckTimeout);
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
    connection.dispose();
  }

  @Override
  public boolean isDisposed() {
    return connection.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return connection.onClose();
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

    final int streamId = streamIdSupplier.nextStreamId(receivers);

    return UnicastMonoEmpty.newInstance(
        () -> {
          ByteBuf requestFrame =
              RequestFireAndForgetFrameFlyweight.encode(
                  allocator,
                  streamId,
                  false,
                  payload.hasMetadata() ? payload.sliceMetadata().retain() : null,
                  payload.sliceData().retain());
          payload.release();

          sendProcessor.onNext(requestFrame);
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

    int streamId = streamIdSupplier.nextStreamId(receivers);
    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;

    UnicastMonoProcessor<Payload> receiver =
        UnicastMonoProcessor.create(
            new MonoLifecycleHandler<Payload>() {
              @Override
              public void doOnSubscribe() {
                final ByteBuf requestFrame =
                    RequestResponseFrameFlyweight.encode(
                        allocator,
                        streamId,
                        false,
                        payload.sliceMetadata().retain(),
                        payload.sliceData().retain());
                payload.release();

                sendProcessor.onNext(requestFrame);
              }

              @Override
              public void doOnTerminal(
                  @Nonnull SignalType signalType,
                  @Nullable Payload element,
                  @Nullable Throwable e) {
                if (signalType == SignalType.ON_ERROR) {
                  sendProcessor.onNext(ErrorFrameFlyweight.encode(allocator, streamId, e));
                } else if (signalType == SignalType.CANCEL) {
                  sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
                }
                removeStreamReceiver(streamId);
              }
            });
    receivers.put(streamId, receiver);

    return receiver.doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER);
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

    int streamId = streamIdSupplier.nextStreamId(receivers);

    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;
    final UnicastProcessor<Payload> receiver =
        UnicastProcessor.create(new CleanOnClearQueueDecorator(Queues.<Payload>unbounded().get()));
    final AtomicBoolean payloadReleasedFlag = new AtomicBoolean(false);

    receivers.put(streamId, receiver);

    return receiver
        .doOnRequest(
            new LongConsumer() {

              boolean firstRequest = true;

              @Override
              public void accept(long n) {
                if (firstRequest && !receiver.isDisposed()) {
                  firstRequest = false;
                  sendProcessor.onNext(
                      RequestStreamFrameFlyweight.encode(
                          allocator,
                          streamId,
                          false,
                          n,
                          payload.sliceMetadata().retain(),
                          payload.sliceData().retain()));
                  if (!payloadReleasedFlag.getAndSet(true)) {
                    payload.release();
                  }
                } else if (contains(streamId) && !receiver.isDisposed()) {
                  sendProcessor.onNext(RequestNFrameFlyweight.encode(allocator, streamId, n));
                }
              }
            })
        .doOnError(
            t -> {
              if (contains(streamId) && !receiver.isDisposed()) {
                sendProcessor.onNext(ErrorFrameFlyweight.encode(allocator, streamId, t));
              }
            })
        .doOnCancel(
            () -> {
              if (!payloadReleasedFlag.getAndSet(true)) {
                payload.release();
              }
              if (contains(streamId) && !receiver.isDisposed()) {
                sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
              }
            })
        .doFinally(s -> removeStreamReceiver(streamId))
        .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER);
  }

  private Flux<Payload> handleChannel(Flux<Payload> request) {
    Throwable err = checkAvailable();
    if (err != null) {
      return Flux.error(err);
    }

    return request.switchOnFirst(
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
        false);
  }

  private Flux<? extends Payload> handleChannel(Payload initialPayload, Flux<Payload> inboundFlux) {
    final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;
    final AtomicBoolean payloadReleasedFlag = new AtomicBoolean(false);
    final int streamId = streamIdSupplier.nextStreamId(receivers);

    final UnicastProcessor<Payload> receiver =
        UnicastProcessor.create(new CleanOnClearQueueDecorator(Queues.<Payload>unbounded().get()));
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
              first = false;
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
                PayloadFrameFlyweight.encode(allocator, streamId, false, false, true, payload);

            sendProcessor.onNext(frame);
            payload.release();
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

    return receiver
        .doOnRequest(
            new LongConsumer() {

              boolean firstRequest = true;

              @Override
              public void accept(long n) {
                if (firstRequest) {
                  firstRequest = false;
                  senders.put(streamId, upstreamSubscriber);
                  receivers.put(streamId, receiver);

                  inboundFlux
                      .transform(f -> applyLimitRateOperator(f, Queues.SMALL_BUFFER_SIZE))
                      .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER)
                      .subscribe(upstreamSubscriber);
                  if (!payloadReleasedFlag.getAndSet(true)) {
                    ByteBuf frame =
                        RequestChannelFrameFlyweight.encode(
                            allocator,
                            streamId,
                            false,
                            false,
                            n,
                            initialPayload.sliceMetadata().retain(),
                            initialPayload.sliceData().retain());

                    sendProcessor.onNext(frame);

                    initialPayload.release();
                  }
                } else {
                  sendProcessor.onNext(RequestNFrameFlyweight.encode(allocator, streamId, n));
                }
              }
            })
        .doOnError(
            t -> {
              if (receivers.remove(streamId, receiver)) {
                upstreamSubscriber.cancel();
              }
            })
        .doOnComplete(() -> receivers.remove(streamId, receiver))
        .doOnCancel(
            () -> {
              if (!payloadReleasedFlag.getAndSet(true)) {
                initialPayload.release();
              }
              if (receivers.remove(streamId, receiver)) {
                sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
                upstreamSubscriber.cancel();
              }
            })
        .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER);
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

    return UnicastMonoEmpty.newInstance(
        () -> {
          ByteBuf metadataPushFrame =
              MetadataPushFrameFlyweight.encode(allocator, payload.sliceMetadata().retain());
          payload.release();

          sendProcessor.onNextPrioritized(metadataPushFrame);
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

  private boolean contains(int streamId) {
    return receivers.containsKey(streamId);
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
    if (receiver == null) {
      handleMissingResponseProcessor(streamId, type, frame);
    } else {
      switch (type) {
        case ERROR:
          receiver.onError(Exceptions.from(streamId, frame));
          receivers.remove(streamId);
          break;
        case NEXT_COMPLETE:
          receiver.onNext(payloadDecoder.apply(frame));
          receiver.onComplete();
          break;
        case CANCEL:
          {
            Subscription sender = senders.remove(streamId);
            if (sender != null) {
              sender.cancel();
            }
            break;
          }
        case NEXT:
          receiver.onNext(payloadDecoder.apply(frame));
          break;
        case REQUEST_N:
          {
            Subscription sender = senders.get(streamId);
            if (sender != null) {
              int n = RequestNFrameFlyweight.requestN(frame);
              sender.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
            }
            break;
          }
        case COMPLETE:
          receiver.onComplete();
          receivers.remove(streamId);
          break;
        default:
          throw new IllegalStateException(
              "Client received supported frame on stream " + streamId + ": " + frame.toString());
      }
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

  private void tryTerminateOnConnectionClose() {
    tryTerminate(() -> CLOSED_CHANNEL_EXCEPTION);
  }

  private void tryTerminateOnZeroError(ByteBuf errorFrame) {
    tryTerminate(() -> Exceptions.from(0, errorFrame));
  }

  private void tryTerminate(Supplier<Exception> errorSupplier) {
    if (terminationError == null) {
      Exception e = errorSupplier.get();
      if (TERMINATION_ERROR.compareAndSet(this, null, e)) {
        terminate(e);
      }
    }
  }

  private void terminate(Exception e) {
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
  }

  private void removeStreamReceiver(int streamId) {
    /*on termination receivers are explicitly cleared to avoid removing from map while iterating over one
    of its views*/
    if (terminationError == null) {
      receivers.remove(streamId);
    }
  }

  private void handleSendProcessorError(Throwable t) {
    connection.dispose();
  }
}
