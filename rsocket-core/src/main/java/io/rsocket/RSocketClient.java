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

import static io.rsocket.keepalive.KeepAliveSupport.ClientKeepAliveSupport;
import static io.rsocket.keepalive.KeepAliveSupport.KeepAlive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.exceptions.Exceptions;
import io.rsocket.frame.*;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.internal.UnicastMonoProcessor;
import io.rsocket.keepalive.KeepAliveFramesAcceptor;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.keepalive.KeepAliveSupport;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.*;

/** Client Side of a RSocket socket. Sends {@link ByteBuf}s to a {@link RSocketServer} */
class RSocketClient implements RSocket {

  private final DuplexConnection connection;
  private final PayloadDecoder payloadDecoder;
  private final Consumer<Throwable> errorConsumer;
  private final StreamIdSupplier streamIdSupplier;
  private final Map<Integer, LimitableRequestPublisher> senders;
  private final Map<Integer, Processor<Payload, Payload>> receivers;
  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final Lifecycle lifecycle = new Lifecycle();
  private final ByteBufAllocator allocator;
  private final KeepAliveFramesAcceptor keepAliveFramesAcceptor;

  /*client requester*/
  RSocketClient(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      StreamIdSupplier streamIdSupplier,
      int keepAliveTickPeriod,
      int keepAliveAckTimeout,
      KeepAliveHandler keepAliveHandler) {
    this.allocator = allocator;
    this.connection = connection;
    this.payloadDecoder = payloadDecoder;
    this.errorConsumer = errorConsumer;
    this.streamIdSupplier = streamIdSupplier;
    this.senders = Collections.synchronizedMap(new IntObjectHashMap<>());
    this.receivers = Collections.synchronizedMap(new IntObjectHashMap<>());

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    this.sendProcessor = new UnboundedProcessor<>();

    connection.onClose().doFinally(signalType -> terminate()).subscribe(null, errorConsumer);

    sendProcessor
        .doOnRequest(
            r -> {
              for (LimitableRequestPublisher lrp : senders.values()) {
                lrp.increaseInternalLimit(r);
              }
            })
        .transform(connection::send)
        .doFinally(this::handleSendProcessorCancel)
        .subscribe(null, this::handleSendProcessorError);

    connection.receive().subscribe(this::handleIncomingFrames, errorConsumer);

    if (keepAliveTickPeriod != 0 && keepAliveHandler != null) {
      KeepAliveSupport keepAliveSupport =
          new ClientKeepAliveSupport(allocator, keepAliveTickPeriod, keepAliveAckTimeout);
      this.keepAliveFramesAcceptor =
          keepAliveHandler.start(keepAliveSupport, sendProcessor::onNext, this::terminate);
    } else {
      keepAliveFramesAcceptor = null;
    }
  }

  /*server requester*/
  RSocketClient(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      StreamIdSupplier streamIdSupplier) {
    this(allocator, connection, payloadDecoder, errorConsumer, streamIdSupplier, 0, 0, null);
  }

  private void terminate(KeepAlive keepAlive) {
    String message =
        String.format("No keep-alive acks for %d ms", keepAlive.getTimeout().toMillis());
    ConnectionErrorException err = new ConnectionErrorException(message);
    lifecycle.setTerminationError(err);
    errorConsumer.accept(err);
    connection.dispose();
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
    return lifecycle.active(
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
        });
  }

  private Mono<Payload> handleRequestResponse(final Payload payload) {
    return lifecycle.activeMono(
        () -> {
          int streamId = streamIdSupplier.nextStreamId();
          final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;
          final ByteBuf requestFrame =
              RequestResponseFrameFlyweight.encode(
                  allocator,
                  streamId,
                  false,
                  payload.sliceMetadata().retain(),
                  payload.sliceData().retain());

          payload.release();

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
        });
  }

  private Flux<Payload> handleRequestStream(final Payload payload) {
    return lifecycle.activeFlux(
        () -> {
          int streamId = streamIdSupplier.nextStreamId();

          final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;
          final UnicastProcessor<Payload> receiver = UnicastProcessor.create();

          receivers.put(streamId, receiver);

          return receiver
              .doOnRequest(
                  new LongConsumer() {

                    // No need to make it atomic; See
                    // https://github.com/reactive-streams/reactive-streams-jvm#2.7
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
                        payload.release();
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
                    if (contains(streamId) && !receiver.isDisposed()) {
                      sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId));
                    }
                  })
              .doFinally(s -> receivers.remove(streamId));
        });
  }

  private Flux<Payload> handleChannel(Flux<Payload> request) {
    return lifecycle.activeFlux(
        () -> {
          final UnboundedProcessor<ByteBuf> sendProcessor = this.sendProcessor;
          final UnicastProcessor<Payload> receiver = UnicastProcessor.create();
          final int streamId = streamIdSupplier.nextStreamId();

          return receiver
              .doOnRequest(
                  new LongConsumer() {

                    // No need to make it atomic; See
                    // https://github.com/reactive-streams/reactive-streams-jvm#2.7
                    boolean firstRequest = true;

                    @Override
                    public void accept(long n) {
                      if (firstRequest) {
                        firstRequest = false;
                        request
                            .transform(
                                f -> {
                                  LimitableRequestPublisher<Payload> wrapped =
                                      LimitableRequestPublisher.wrap(f, sendProcessor.available());
                                  // Need to set this to one for first the frame
                                  wrapped.request(1);
                                  senders.put(streamId, wrapped);
                                  receivers.put(streamId, receiver);

                                  return wrapped;
                                })
                            .subscribe(
                                new BaseSubscriber<Payload>() {

                                  // no need to make it atomic; See
                                  // https://github.com/reactive-streams/reactive-streams-jvm#1.3
                                  boolean firstPayload = true;

                                  @Override
                                  protected void hookOnNext(Payload payload) {
                                    final ByteBuf frame;

                                    if (firstPayload) {
                                      firstPayload = false;
                                      frame =
                                          RequestChannelFrameFlyweight.encode(
                                              allocator,
                                              streamId,
                                              false,
                                              false,
                                              n,
                                              payload.sliceMetadata().retain(),
                                              payload.sliceData().retain());
                                    } else {
                                      frame =
                                          PayloadFrameFlyweight.encode(
                                              allocator, streamId, false, false, true, payload);
                                    }

                                    sendProcessor.onNext(frame);
                                    payload.release();
                                  }

                                  @Override
                                  protected void hookOnComplete() {
                                    if (contains(streamId) && !receiver.isDisposed()) {
                                      sendProcessor.onNext(
                                          PayloadFrameFlyweight.encodeComplete(
                                              allocator, streamId));
                                    }
                                    if (firstPayload) {
                                      receiver.onComplete();
                                    }
                                  }

                                  @Override
                                  protected void hookOnError(Throwable t) {
                                    errorConsumer.accept(t);
                                    receiver.dispose();
                                  }
                                });
                      } else {
                        if (contains(streamId) && !receiver.isDisposed()) {
                          sendProcessor.onNext(
                              RequestNFrameFlyweight.encode(allocator, streamId, n));
                        }
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
        });
  }

  private Mono<Void> handleMetadataPush(Payload payload) {
    return lifecycle.active(
        () -> {
          sendProcessor.onNext(
              MetadataPushFrameFlyweight.encode(allocator, payload.sliceMetadata().retain()));
        });
  }

  private boolean contains(int streamId) {
    return receivers.containsKey(streamId);
  }

  protected void terminate() {
    lifecycle.setTerminationError(new ClosedChannelException());

    if (keepAliveFramesAcceptor != null) {
      keepAliveFramesAcceptor.dispose();
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
      frame.release();
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(frame);
      throw reactor.core.Exceptions.propagate(t);
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
          receiver.onError(Exceptions.from(frame));
          receivers.remove(streamId);
          break;
        case NEXT_COMPLETE:
          receiver.onNext(payloadDecoder.apply(frame));
          receiver.onComplete();
          break;
        case CANCEL:
          {
            LimitableRequestPublisher sender = senders.remove(streamId);
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
            LimitableRequestPublisher sender = senders.get(streamId);
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

  private static class Lifecycle {

    private static final AtomicReferenceFieldUpdater<Lifecycle, Throwable> TERMINATION_ERROR =
        AtomicReferenceFieldUpdater.newUpdater(
            Lifecycle.class, Throwable.class, "terminationError");
    private volatile Throwable terminationError;

    public Mono<Void> active(Runnable runnable) {
      return Mono.create(
          sink -> {
            if (terminationError == null) {
              runnable.run();
              sink.success();
            } else {
              sink.error(terminationError);
            }
          });
    }

    public <T> Mono<T> activeMono(Supplier<? extends Mono<? extends T>> supplier) {
      return Mono.defer(
          () -> {
            if (terminationError == null) {
              return supplier.get();
            } else {
              return Mono.error(terminationError);
            }
          });
    }

    public <T> Flux<T> activeFlux(Supplier<? extends Flux<T>> supplier) {
      return Flux.defer(
          () -> {
            if (terminationError == null) {
              return supplier.get();
            } else {
              return Flux.error(terminationError);
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
