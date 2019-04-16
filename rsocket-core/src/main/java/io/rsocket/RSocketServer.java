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

import static io.rsocket.keepalive.KeepAliveSupport.KeepAlive;
import static io.rsocket.keepalive.KeepAliveSupport.ServerKeepAliveSupport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.*;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.keepalive.KeepAliveFramesAcceptor;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.keepalive.KeepAliveSupport;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.*;

/** Server side RSocket. Receives {@link ByteBuf}s from a {@link RSocketClient} */
class RSocketServer implements ResponderRSocket {

  private final DuplexConnection connection;
  private final RSocket requestHandler;
  private final ResponderRSocket responderRSocket;
  private final PayloadDecoder payloadDecoder;
  private final Consumer<Throwable> errorConsumer;

  private final Map<Integer, LimitableRequestPublisher> sendingLimitableSubscriptions;
  private final Map<Integer, Subscription> sendingSubscriptions;
  private final Map<Integer, Processor<Payload, Payload>> channelProcessors;

  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final ByteBufAllocator allocator;
  private final KeepAliveFramesAcceptor keepAliveFramesAcceptor;

  /*client responder*/
  RSocketServer(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer) {
    this(allocator, connection, requestHandler, payloadDecoder, errorConsumer, 0, 0, null);
  }

  /*server responder*/
  RSocketServer(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      int keepAliveTickPeriod,
      int keepAliveAckTimeout,
      KeepAliveHandler keepAliveHandler) {
    this.allocator = allocator;
    this.connection = connection;

    this.requestHandler = requestHandler;
    this.responderRSocket =
        (requestHandler instanceof ResponderRSocket) ? (ResponderRSocket) requestHandler : null;

    this.payloadDecoder = payloadDecoder;
    this.errorConsumer = errorConsumer;
    this.sendingLimitableSubscriptions = Collections.synchronizedMap(new IntObjectHashMap<>());
    this.sendingSubscriptions = Collections.synchronizedMap(new IntObjectHashMap<>());
    this.channelProcessors = Collections.synchronizedMap(new IntObjectHashMap<>());

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    // connections
    this.sendProcessor = new UnboundedProcessor<>();

    sendProcessor
        .doOnRequest(
            r -> {
              for (LimitableRequestPublisher lrp : sendingLimitableSubscriptions.values()) {
                lrp.increaseInternalLimit(r);
              }
            })
        .transform(connection::send)
        .doFinally(this::handleSendProcessorCancel)
        .subscribe(null, this::handleSendProcessorError);

    Disposable receiveDisposable = connection.receive().subscribe(this::handleFrame, errorConsumer);

    this.connection
        .onClose()
        .doFinally(
            s -> {
              cleanup();
              receiveDisposable.dispose();
            })
        .subscribe(null, errorConsumer);

    if (keepAliveTickPeriod != 0 && keepAliveHandler != null) {
      KeepAliveSupport keepAliveSupport =
          new ServerKeepAliveSupport(allocator, keepAliveTickPeriod, keepAliveAckTimeout);
      keepAliveFramesAcceptor =
          keepAliveHandler.start(keepAliveSupport, sendProcessor::onNext, this::terminate);
    } else {
      keepAliveFramesAcceptor = null;
    }
  }

  private void terminate(KeepAlive keepAlive) {
    String message =
        String.format("No keep-alive acks for %d ms", keepAlive.getTimeout().toMillis());
    errorConsumer.accept(new ConnectionErrorException(message));
    connection.dispose();
  }

  private void handleSendProcessorError(Throwable t) {
    sendingSubscriptions
        .values()
        .forEach(
            subscription -> {
              try {
                subscription.cancel();
              } catch (Throwable e) {
                errorConsumer.accept(e);
              }
            });

    sendingLimitableSubscriptions
        .values()
        .forEach(
            subscription -> {
              try {
                subscription.cancel();
              } catch (Throwable e) {
                errorConsumer.accept(e);
              }
            });

    channelProcessors
        .values()
        .forEach(
            subscription -> {
              try {
                subscription.onError(t);
              } catch (Throwable e) {
                errorConsumer.accept(e);
              }
            });
  }

  private void handleSendProcessorCancel(SignalType t) {
    if (SignalType.ON_ERROR == t) {
      return;
    }

    sendingSubscriptions
        .values()
        .forEach(
            subscription -> {
              try {
                subscription.cancel();
              } catch (Throwable e) {
                errorConsumer.accept(e);
              }
            });

    sendingLimitableSubscriptions
        .values()
        .forEach(
            subscription -> {
              try {
                subscription.cancel();
              } catch (Throwable e) {
                errorConsumer.accept(e);
              }
            });

    channelProcessors
        .values()
        .forEach(
            subscription -> {
              try {
                subscription.onComplete();
              } catch (Throwable e) {
                errorConsumer.accept(e);
              }
            });
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      return requestHandler.fireAndForget(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      return requestHandler.requestResponse(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      return requestHandler.requestStream(payload);
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    try {
      return requestHandler.requestChannel(payloads);
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Payload payload, Publisher<Payload> payloads) {
    try {
      return responderRSocket.requestChannel(payload, payloads);
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

  private void cleanup() {
    if (keepAliveFramesAcceptor != null) {
      keepAliveFramesAcceptor.dispose();
    }
    cleanUpSendingSubscriptions();
    cleanUpChannelProcessors();

    requestHandler.dispose();
    sendProcessor.dispose();
  }

  private synchronized void cleanUpSendingSubscriptions() {
    sendingSubscriptions.values().forEach(Subscription::cancel);
    sendingSubscriptions.clear();

    sendingLimitableSubscriptions.values().forEach(Subscription::cancel);
    sendingLimitableSubscriptions.clear();
  }

  private synchronized void cleanUpChannelProcessors() {
    channelProcessors.values().forEach(Processor::onComplete);
    channelProcessors.clear();
  }

  private void handleFrame(ByteBuf frame) {
    try {
      int streamId = FrameHeaderFlyweight.streamId(frame);
      Subscriber<Payload> receiver;
      FrameType frameType = FrameHeaderFlyweight.frameType(frame);
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
        case KEEPALIVE:
          handleKeepAliveFrame(frame);
          break;
        case REQUEST_N:
          handleRequestN(streamId, frame);
          break;
        case REQUEST_STREAM:
          handleStream(
              streamId,
              requestStream(payloadDecoder.apply(frame)),
              RequestStreamFrameFlyweight.initialRequestN(frame));
          break;
        case REQUEST_CHANNEL:
          handleChannel(
              streamId,
              payloadDecoder.apply(frame),
              RequestChannelFrameFlyweight.initialRequestN(frame));
          break;
        case METADATA_PUSH:
          metadataPush(payloadDecoder.apply(frame));
          break;
        case PAYLOAD:
          // TODO: Hook in receiving socket.
          break;
        case LEASE:
          // Lease must not be received here as this is the server end of the socket which sends
          // leases.
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
            receiver.onError(new ApplicationErrorException(ErrorFrameFlyweight.dataUtf8(frame)));
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
          protected void hookOnError(Throwable throwable) {
            errorConsumer.accept(throwable);
          }

          @Override
          protected void hookFinally(SignalType type) {
            sendingSubscriptions.remove(streamId);
          }
        });
  }

  private void handleRequestResponse(int streamId, Mono<Payload> response) {
    response.subscribe(
        new BaseSubscriber<Payload>() {
          private boolean isEmpty = true;

          @Override
          protected void hookOnSubscribe(Subscription subscription) {
            sendingSubscriptions.put(streamId, subscription);
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          protected void hookOnNext(Payload payload) {
            if (isEmpty) {
              isEmpty = false;
            }

            ByteBuf byteBuf;
            try {
              byteBuf = PayloadFrameFlyweight.encodeNextComplete(allocator, streamId, payload);
            } catch (Throwable t) {
              payload.release();
              throw Exceptions.propagate(t);
            }

            payload.release();

            sendProcessor.onNext(byteBuf);
          }

          @Override
          protected void hookOnError(Throwable throwable) {
            handleError(streamId, throwable);
          }

          @Override
          protected void hookOnComplete() {
            if (isEmpty) {
              sendProcessor.onNext(PayloadFrameFlyweight.encodeComplete(allocator, streamId));
            }
          }

          @Override
          protected void hookFinally(SignalType type) {
            sendingSubscriptions.remove(streamId);
          }
        });
  }

  private void handleStream(int streamId, Flux<Payload> response, int initialRequestN) {
    response
        .transform(
            frameFlux -> {
              LimitableRequestPublisher<Payload> payloads =
                  LimitableRequestPublisher.wrap(frameFlux, sendProcessor.available());
              sendingLimitableSubscriptions.put(streamId, payloads);
              payloads.request(
                  initialRequestN >= Integer.MAX_VALUE ? Long.MAX_VALUE : initialRequestN);
              return payloads;
            })
        .subscribe(
            new BaseSubscriber<Payload>() {

              @Override
              protected void hookOnNext(Payload payload) {
                ByteBuf byteBuf;
                try {
                  byteBuf = PayloadFrameFlyweight.encodeNext(allocator, streamId, payload);
                } catch (Throwable t) {
                  payload.release();
                  throw Exceptions.propagate(t);
                }

                payload.release();

                sendProcessor.onNext(byteBuf);
              }

              @Override
              protected void hookOnComplete() {
                sendProcessor.onNext(PayloadFrameFlyweight.encodeComplete(allocator, streamId));
              }

              @Override
              protected void hookOnError(Throwable throwable) {
                handleError(streamId, throwable);
              }

              @Override
              protected void hookFinally(SignalType type) {
                sendingLimitableSubscriptions.remove(streamId);
              }
            });
  }

  private void handleChannel(int streamId, Payload payload, int initialRequestN) {
    UnicastProcessor<Payload> frames = UnicastProcessor.create();
    channelProcessors.put(streamId, frames);

    Flux<Payload> payloads =
        frames
            .doOnCancel(
                () -> sendProcessor.onNext(CancelFrameFlyweight.encode(allocator, streamId)))
            .doOnError(t -> handleError(streamId, t))
            .doOnRequest(
                l -> sendProcessor.onNext(RequestNFrameFlyweight.encode(allocator, streamId, l)))
            .doFinally(signalType -> channelProcessors.remove(streamId));

    // not chained, as the payload should be enqueued in the Unicast processor before this method
    // returns
    // and any later payload can be processed
    frames.onNext(payload);

    if (responderRSocket != null) {
      handleStream(streamId, requestChannel(payload, payloads), initialRequestN);
    } else {
      handleStream(streamId, requestChannel(payloads), initialRequestN);
    }
  }

  private void handleKeepAliveFrame(ByteBuf frame) {
    if (keepAliveFramesAcceptor != null) {
      keepAliveFramesAcceptor.receive(frame);
    }
  }

  private void handleCancelFrame(int streamId) {
    Subscription subscription = sendingSubscriptions.remove(streamId);

    if (subscription == null) {
      subscription = sendingLimitableSubscriptions.get(streamId);
    }

    if (subscription != null) {
      subscription.cancel();
    }
  }

  private void handleError(int streamId, Throwable t) {
    errorConsumer.accept(t);
    sendProcessor.onNext(ErrorFrameFlyweight.encode(allocator, streamId, t));
  }

  private void handleRequestN(int streamId, ByteBuf frame) {
    Subscription subscription = sendingSubscriptions.get(streamId);

    if (subscription == null) {
      subscription = sendingLimitableSubscriptions.get(streamId);
    }

    if (subscription != null) {
      int n = RequestNFrameFlyweight.requestN(frame);
      subscription.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
    }
  }
}
