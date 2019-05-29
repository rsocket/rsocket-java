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

import io.netty.util.collection.IntObjectHashMap;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.exceptions.Exceptions;
import io.rsocket.framing.FrameType;
import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.internal.UnicastMonoProcessor;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.*;

/** Client Side of a RSocket socket. Sends {@link Frame}s to a {@link RSocketServer} */
class RSocketClient implements RSocket {

  private final DuplexConnection connection;
  private final Function<Frame, ? extends Payload> frameDecoder;
  private final Consumer<Throwable> errorConsumer;
  private final StreamIdSupplier streamIdSupplier;
  private final Map<Integer, LimitableRequestPublisher> senders;
  private final Map<Integer, Processor<Payload, Payload>> receivers;
  private final UnboundedProcessor<Frame> sendProcessor;
  private final Lifecycle lifecycle = new Lifecycle();
  private KeepAliveHandler keepAliveHandler;

  /*server requester*/
  RSocketClient(
      DuplexConnection connection,
      Function<Frame, ? extends Payload> frameDecoder,
      Consumer<Throwable> errorConsumer,
      StreamIdSupplier streamIdSupplier) {
    this(
        connection, frameDecoder, errorConsumer, streamIdSupplier, Duration.ZERO, Duration.ZERO, 0);
  }

  /*client requester*/
  RSocketClient(
      DuplexConnection connection,
      Function<Frame, ? extends Payload> frameDecoder,
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
        .send(
            sendProcessor.doOnRequest(
                r -> {
                  for (LimitableRequestPublisher lrp : senders.values()) {
                    lrp.increaseInternalLimit(r);
                  }
                }))
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
                  final Frame requestFrame =
                      Frame.Request.from(streamId, FrameType.REQUEST_FNF, payload, 1);
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
                              final Frame requestFrame =
                                  Frame.Request.from(
                                      streamId, FrameType.REQUEST_STREAM, payload, n);
                              payload.release();
                              sendProcessor.onNext(requestFrame);
                            } else if (contains(streamId) && !receiver.isDisposed()) {
                              sendProcessor.onNext(Frame.RequestN.from(streamId, n));
                            }
                            sendProcessor.drain();
                          })
                      .doOnError(
                          t -> {
                            if (contains(streamId) && !receiver.isDisposed()) {
                              sendProcessor.onNext(Frame.Error.from(streamId, t));
                            }
                          })
                      .doOnCancel(
                          () -> {
                            if (contains(streamId) && !receiver.isDisposed()) {
                              sendProcessor.onNext(Frame.Cancel.from(streamId));
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
                  final Frame requestFrame =
                      Frame.Request.from(streamId, FrameType.REQUEST_RESPONSE, payload, 1);
                  payload.release();

                  UnicastMonoProcessor<Payload> receiver = UnicastMonoProcessor.create();
                  receivers.put(streamId, receiver);

                  sendProcessor.onNext(requestFrame);

                  return receiver
                      .doOnError(t -> sendProcessor.onNext(Frame.Error.from(streamId, t)))
                      .doFinally(
                          s -> {
                            if (s == SignalType.CANCEL) {
                              sendProcessor.onNext(Frame.Cancel.from(streamId));
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
                              final Flux<Frame> requestFrames =
                                  request
                                      .transform(
                                          f -> {
                                            LimitableRequestPublisher<Payload> wrapped =
                                                LimitableRequestPublisher.wrap(
                                                    f, sendProcessor.available());
                                            // Need to set this to one for first the frame
                                            wrapped.increaseRequestLimit(1);
                                            senders.put(streamId, wrapped);
                                            receivers.put(streamId, receiver);

                                            return wrapped;
                                          })
                                      .map(
                                          payload -> {
                                            final Frame requestFrame;
                                            if (firstPayload.compareAndSet(true, false)) {
                                              requestFrame =
                                                  Frame.Request.from(
                                                      streamId,
                                                      FrameType.REQUEST_CHANNEL,
                                                      payload,
                                                      n);
                                            } else {
                                              requestFrame =
                                                  Frame.PayloadFrame.from(
                                                      streamId, FrameType.NEXT, payload);
                                            }
                                            payload.release();
                                            return requestFrame;
                                          })
                                      .doOnComplete(
                                          () -> {
                                            if (contains(streamId) && !receiver.isDisposed()) {
                                              sendProcessor.onNext(
                                                  Frame.PayloadFrame.from(
                                                      streamId, FrameType.COMPLETE));
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
                                sendProcessor.onNext(Frame.RequestN.from(streamId, n));
                              }
                            }
                          })
                      .doOnError(
                          t -> {
                            if (contains(streamId) && !receiver.isDisposed()) {
                              sendProcessor.onNext(Frame.Error.from(streamId, t));
                            }
                          })
                      .doOnCancel(
                          () -> {
                            if (contains(streamId) && !receiver.isDisposed()) {
                              sendProcessor.onNext(Frame.Cancel.from(streamId));
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
                  final Frame requestFrame =
                      Frame.Request.from(0, FrameType.METADATA_PUSH, payload, 1);
                  payload.release();
                  sendProcessor.onNext(requestFrame);
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

  private void handleIncomingFrames(Frame frame) {
    try {
      int streamId = frame.getStreamId();
      FrameType type = frame.getType();
      if (streamId == 0) {
        handleStreamZero(type, frame);
      } else {
        handleFrame(streamId, type, frame);
      }
    } finally {
      frame.release();
    }
  }

  private void handleStreamZero(FrameType type, Frame frame) {
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

  private void handleFrame(int streamId, FrameType type, Frame frame) {
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
              int n = Frame.RequestN.requestN(frame);
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

  private void handleMissingResponseProcessor(int streamId, FrameType type, Frame frame) {
    if (!streamIdSupplier.isBeforeOrCurrent(streamId)) {
      if (type == FrameType.ERROR) {
        // message for stream that has never existed, we have a problem with
        // the overall connection and must tear down
        String errorMessage = frame.getDataUtf8();

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

    public void setTerminationError(Throwable err) {
      TERMINATION_ERROR.compareAndSet(this, null, err);
    }

    public Throwable getTerminationError() {
      return terminationError;
    }
  }
}
