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

package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.collection.IntObjectHashMap;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.exceptions.Exceptions;
import io.rsocket.frame.*;
import io.rsocket.frame.decoder.FrameDecoder;
import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.internal.UnicastMonoProcessor;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.UnicastProcessor;

import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

/** Client Side of a RSocket socket. Sends {@link ByteBuf}s to a {@link RSocketServer} */
class RSocketClient implements RSocket {

  private final DuplexConnection connection;
  private final FrameDecoder frameDecoder;
  private final Consumer<Throwable> errorConsumer;
  private final StreamIdSupplier streamIdSupplier;
  private final Map<Integer, LimitableRequestPublisher> senders;
  private final Map<Integer, Processor<Payload, Payload>> receivers;
  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final Lifecycle lifecycle = new Lifecycle();
  private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
  private KeepAliveHandler keepAliveHandler;

  /*server requester*/
  RSocketClient(
      DuplexConnection connection,
      FrameDecoder frameDecoder,
      Consumer<Throwable> errorConsumer,
      StreamIdSupplier streamIdSupplier) {
    this(
        connection, frameDecoder, errorConsumer, streamIdSupplier, Duration.ZERO, Duration.ZERO, 0);
  }

  /*client requester*/
  RSocketClient(
      DuplexConnection connection,
      FrameDecoder frameDecoder,
      Consumer<Throwable> errorConsumer,
      StreamIdSupplier streamIdSupplier,
      Duration tickPeriod,
      Duration ackTimeout,
      int missedAcks) {
    this.connection = connection;
    this.frameDecoder = frameDecoder;
    this.errorConsumer = errorConsumer;
    this.streamIdSupplier = streamIdSupplier;
    this.senders = Collections.synchronizedMap(new IntObjectHashMap<>());
    this.receivers = Collections.synchronizedMap(new IntObjectHashMap<>());

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    this.sendProcessor = new UnboundedProcessor<>();

    connection.onClose().doFinally(signalType -> terminate()).subscribe(null, errorConsumer);

    connection
        .send(sendProcessor)
        .doFinally(this::handleSendProcessorCancel)
        .subscribe(null, this::handleSendProcessorError);

    connection.receive().subscribe(this::handleIncomingFrames, errorConsumer);

    if (!Duration.ZERO.equals(tickPeriod)) {
      this.keepAliveHandler =
          KeepAliveHandler.ofClient(
              new KeepAliveHandler.KeepAlive(tickPeriod, ackTimeout, missedAcks));

      keepAliveHandler
          .timeout()
          .subscribe(
              keepAlive -> {
                String message =
                    String.format("No keep-alive acks for %d ms", keepAlive.getTimeoutMillis());
                ConnectionErrorException err = new ConnectionErrorException(message);
                lifecycle.setTerminationError(err);
                errorConsumer.accept(err);
                connection.dispose();
              });
      keepAliveHandler.send().subscribe(sendProcessor::onNext);
    } else {
      keepAliveHandler = null;
    }
  }

  private void handleSendProcessorError(Throwable t) {
    Throwable terminationError = lifecycle.getTerminationError();
    Throwable err = terminationError != null ? terminationError : t;
    receivers
        .values()
        .forEach(
            subscriber -> {
              try {
                subscriber.onError(err);
              } catch (Throwable e) {
                errorConsumer.accept(e);
              }
            });

    senders.values().forEach(LimitableRequestPublisher::cancel);
  }

  private void handleSendProcessorCancel(SignalType t) {
    if (SignalType.ON_ERROR == t) {
      return;
    }

    receivers
        .values()
        .forEach(
            subscriber -> {
              try {
                subscriber.onError(new Throwable("closed connection"));
              } catch (Throwable e) {
                errorConsumer.accept(e);
              }
            });

    senders.values().forEach(LimitableRequestPublisher::cancel);
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
    return connection.availability();
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
    return lifecycle
        .active()
        .then(
            Mono.fromRunnable(
                () -> {
                  final int streamId = streamIdSupplier.nextStreamId();
                  ByteBuf requestFrame =
                      RequestFireAndForgetFrameFlyweight.encode(
                          allocator,
                          streamId,
                          false,
                          payload.hasMetadata() ? payload.sliceMetadata().retain() : null,
                          payload.sliceData().retain());

                  payload.release();
                  sendProcessor.onNext(requestFrame);
                }));
  }

  private Flux<Payload> handleRequestStream(final Payload payload) {
    return lifecycle
        .active()
        .thenMany(
            Flux.defer(
                () -> {
                  int streamId = streamIdSupplier.nextStreamId();

                  UnicastProcessor<Payload> receiver = UnicastProcessor.create();
                  receivers.put(streamId, receiver);

                  AtomicBoolean first = new AtomicBoolean(false);

                  return receiver
                      .doOnRequest(
                          n -> {
                            if (first.compareAndSet(false, true) && !receiver.isDisposed()) {
                              sendProcessor.onNext(
                                  RequestStreamFrameFlyweight.encode(
                                      allocator,
                                      streamId,
                                      false,
                                      (int) n,
                                      payload.sliceMetadata().retain(),
                                      payload.sliceData().retain()));
                            } else if (contains(streamId) && !receiver.isDisposed()) {
                              sendProcessor.onNext(
                                  RequestNFrameFlyweight.encode(allocator, streamId, n));
                            }
                            sendProcessor.drain();
                          })
                      .doOnError(
                          t -> {
                            if (contains(streamId) && !receiver.isDisposed()) {
                              sendProcessor.onNext(ErrorFrameFlyweight.encode(allocator, streamId, t));
                            }
                          })
                      .doOnCancel(
                          () -> {
                            if (contains(streamId) && !receiver.isDisposed()) {
                              sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
                            }
                          })
                      .doFinally(
                          s -> {
                            receivers.remove(streamId);
                          });
                }));
  }

  private Mono<Payload> handleRequestResponse(final Payload payload) {
    return lifecycle
        .active()
        .then(
            Mono.defer(
                () -> {
                  int streamId = streamIdSupplier.nextStreamId();
                  ByteBuf requestFrame =
                      RequestResponseFrameFlyweight.encode(
                          allocator,
                          streamId,
                          false,
                          payload.sliceMetadata().retain(),
                          payload.sliceData().retain());

                  UnicastMonoProcessor<Payload> receiver = UnicastMonoProcessor.create();
                  receivers.put(streamId, receiver);

                  sendProcessor.onNext(requestFrame);

                  return receiver
                      .doOnError(
                          t -> sendProcessor.onNext(ErrorFrameFlyweight.encode(allocator, streamId, t)))
                      .doFinally(
                          s -> {
                            if (s == SignalType.CANCEL) {
                              sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
                            }

                            receivers.remove(streamId);
                          });
                }));
  }

  private Flux<Payload> handleChannel(Flux<Payload> request) {
    return lifecycle
        .active()
        .thenMany(
            Flux.defer(
                () -> {
                  final UnicastProcessor<Payload> receiver = UnicastProcessor.create();
                  final int streamId = streamIdSupplier.nextStreamId();
                  final AtomicBoolean firstRequest = new AtomicBoolean(true);

                  return receiver
                      .doOnRequest(
                          n -> {
                            if (firstRequest.compareAndSet(true, false)) {
                              final AtomicBoolean firstPayload = new AtomicBoolean(true);
                              final Flux<ByteBuf> requestFrames =
                                  request
                                      .transform(
                                          f -> {
                                            LimitableRequestPublisher<Payload> wrapped =
                                                LimitableRequestPublisher.wrap(f);
                                            // Need to set this to one for first the frame
                                            wrapped.increaseRequestLimit(1);
                                            senders.put(streamId, wrapped);
                                            receivers.put(streamId, receiver);

                                            return wrapped;
                                          })
                                      .map(
                                          payload -> {
                                            final ByteBuf requestFrame;
                                            if (firstPayload.compareAndSet(true, false)) {
                                              requestFrame =
                                                  RequestChannelFrameFlyweight.encode(
                                                      allocator,
                                                      streamId,
                                                      false,
                                                      false,
                                                      (int) n,
                                                      payload.sliceMetadata().retain(),
                                                      payload.sliceData().retain());
                                            } else {
                                              requestFrame =
                                                  PayloadFrameFlyweight.encode(
                                                      allocator, streamId, false, false, true,
                                                      payload);
                                            }
                                            return requestFrame;
                                          })
                                      .doOnComplete(
                                          () -> {
                                            if (contains(streamId) && !receiver.isDisposed()) {
                                              sendProcessor.onNext(
                                                  PayloadFrameFlyweight.encodeComplete(
                                                      allocator, streamId));
                                            }
                                            if (firstPayload.get()) {
                                              receiver.onComplete();
                                            }
                                          });

                              requestFrames.subscribe(
                                  sendProcessor::onNext,
                                  t -> {
                                    errorConsumer.accept(t);
                                    receiver.dispose();
                                  });
                            } else {
                              if (contains(streamId) && !receiver.isDisposed()) {
                                sendProcessor.onNext(
                                    RequestNFrameFlyweight.encode(allocator, streamId, n));
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
                            if (contains(streamId) && !receiver.isDisposed()) {
                              sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
                            }
                          })
                      .doFinally(
                          s -> {
                            receivers.remove(streamId);
                            LimitableRequestPublisher sender = senders.remove(streamId);
                            if (sender != null) {
                              sender.cancel();
                            }
                          });
                }));
  }

  private Mono<Void> handleMetadataPush(Payload payload) {
    return lifecycle
        .active()
        .then(
            Mono.fromRunnable(
                () -> {
                  sendProcessor.onNext(
                      MetadataPushFrameFlyweight.encode(allocator, payload.sliceMetadata().retain()));
                }));
  }

  private boolean contains(int streamId) {
    return receivers.containsKey(streamId);
  }

  protected void terminate() {
    lifecycle.setTerminationError(new ClosedChannelException());

    if (keepAliveHandler != null) {
      keepAliveHandler.dispose();
    }
    try {
      receivers.values().forEach(this::cleanUpSubscriber);
      senders.values().forEach(this::cleanUpLimitableRequestPublisher);
    } finally {
      senders.clear();
      receivers.clear();
      sendProcessor.dispose();
    }
  }

  private synchronized void cleanUpLimitableRequestPublisher(
      LimitableRequestPublisher<?> limitableRequestPublisher) {
    try {
      limitableRequestPublisher.cancel();
    } catch (Throwable t) {
      errorConsumer.accept(t);
    }
  }

  private synchronized void cleanUpSubscriber(Processor subscriber) {
    try {
      subscriber.onError(lifecycle.getTerminationError());
    } catch (Throwable t) {
      errorConsumer.accept(t);
    }
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
    } finally {
      frame.release();
    }
  }

  private void handleStreamZero(FrameType type, ByteBuf frame) {
    switch (type) {
      case ERROR:
        RuntimeException error = Exceptions.from(frame);
        lifecycle.setTerminationError(error);
        errorConsumer.accept(error);
        connection.dispose();
        break;
      case LEASE:
        break;
      case KEEPALIVE:
        if (keepAliveHandler != null) {
          keepAliveHandler.receive(frame);
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
          receiver.onError(Exceptions.from(frame));
          receivers.remove(streamId);
          break;
        case NEXT_COMPLETE:
          receiver.onNext(frameDecoder.apply(frame));
          receiver.onComplete();
          break;
        case CANCEL:
          {
            LimitableRequestPublisher sender = senders.remove(streamId);
            receivers.remove(streamId);
            if (sender != null) {
              sender.cancel();
            }
            break;
          }
        case NEXT:
          receiver.onNext(frameDecoder.apply(frame));
          break;
        case REQUEST_N:
          {
            LimitableRequestPublisher sender = senders.get(streamId);
            if (sender != null) {
              int n = RequestNFrameFlyweight.requestN(frame);
              sender.increaseRequestLimit(n);
              sendProcessor.drain();
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

  private static class Lifecycle {

    private static final AtomicReferenceFieldUpdater<Lifecycle, Throwable> TERMINATION_ERROR =
        AtomicReferenceFieldUpdater.newUpdater(
            Lifecycle.class, Throwable.class, "terminationError");
    private volatile Throwable terminationError;

    public Mono<Void> active() {
      return Mono.create(
          sink -> {
            if (terminationError == null) {
              sink.success();
            } else {
              sink.error(terminationError);
            }
          });
    }

    public Throwable getTerminationError() {
      return terminationError;
    }

    public void setTerminationError(Throwable err) {
      TERMINATION_ERROR.compareAndSet(this, null, err);
    }
  }
}
