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
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.frame.*;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.internal.UnboundedProcessor;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.UnicastProcessor;

/** Server side RSocket. Receives {@link ByteBuf}s from a {@link RSocketClient} */
class RSocketServer implements RSocket {

  private final DuplexConnection connection;
  private final RSocket requestHandler;
  private final PayloadDecoder payloadDecoder;
  private final Consumer<Throwable> errorConsumer;

  private final Map<Integer, Subscription> sendingSubscriptions;
  private final Map<Integer, Processor<Payload, Payload>> channelProcessors;

  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final ByteBufAllocator allocator;

  /*server responder*/
  RSocketServer(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer) {
    this.allocator = allocator;
    this.connection = connection;
    this.requestHandler = requestHandler;
    this.payloadDecoder = payloadDecoder;
    this.errorConsumer = errorConsumer;
    this.sendingSubscriptions = Collections.synchronizedMap(new IntObjectHashMap<>());
    this.channelProcessors = Collections.synchronizedMap(new IntObjectHashMap<>());

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    // connections
    this.sendProcessor = new UnboundedProcessor<>();

    connection
        .send(sendProcessor)
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
    cleanUpSendingSubscriptions();
    cleanUpChannelProcessors();

    requestHandler.dispose();
    sendProcessor.dispose();
  }

  private synchronized void cleanUpSendingSubscriptions() {
    sendingSubscriptions.values().forEach(Subscription::cancel);
    sendingSubscriptions.clear();
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
          // KeepAlive is handled by corresponding connection interceptor,
          // just release its frame here
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
    result
        .doOnSubscribe(subscription -> sendingSubscriptions.put(streamId, subscription))
        .doFinally(signalType -> sendingSubscriptions.remove(streamId))
        .subscribe(null, errorConsumer);
  }

  private void handleRequestResponse(int streamId, Mono<Payload> response) {
    response
        .doOnSubscribe(subscription -> sendingSubscriptions.put(streamId, subscription))
        .map(
            payload -> {
              ByteBuf byteBuf = null;
              try {
                byteBuf = PayloadFrameFlyweight.encodeNextComplete(allocator, streamId, payload);
              } catch (Throwable t) {
                if (byteBuf != null) {
                  ReferenceCountUtil.safeRelease(byteBuf);
                  ReferenceCountUtil.safeRelease(payload);
                }
              }
              payload.release();
              return byteBuf;
            })
        .switchIfEmpty(
            Mono.fromCallable(() -> PayloadFrameFlyweight.encodeComplete(allocator, streamId)))
        .doFinally(signalType -> sendingSubscriptions.remove(streamId))
        .subscribe(
            t1 -> {
              sendProcessor.onNext(t1);
            },
            t -> handleError(streamId, t));
  }

  private void handleStream(int streamId, Flux<Payload> response, int initialRequestN) {
    response
        .transform(
            frameFlux -> {
              LimitableRequestPublisher<Payload> payloads =
                  LimitableRequestPublisher.wrap(frameFlux);
              sendingSubscriptions.put(streamId, payloads);
              payloads.increaseRequestLimit(initialRequestN);
              return payloads;
            })
        .doFinally(signalType -> sendingSubscriptions.remove(streamId))
        .subscribe(
            payload -> {
              ByteBuf byteBuf = null;
              try {
                byteBuf = PayloadFrameFlyweight.encodeNext(allocator, streamId, payload);
              } catch (Throwable t) {
                if (byteBuf != null) {
                  ReferenceCountUtil.safeRelease(byteBuf);
                  ReferenceCountUtil.safeRelease(payload);
                }
                throw Exceptions.propagate(t);
              }
              payload.release();
              sendProcessor.onNext(byteBuf);
            },
            t -> handleError(streamId, t),
            () -> sendProcessor.onNext(PayloadFrameFlyweight.encodeComplete(allocator, streamId)));
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

    handleStream(streamId, requestChannel(payloads), initialRequestN);
  }

  private void handleCancelFrame(int streamId) {
    Subscription subscription = sendingSubscriptions.remove(streamId);
    if (subscription != null) {
      subscription.cancel();
    }
  }

  private void handleError(int streamId, Throwable t) {
    errorConsumer.accept(t);
    sendProcessor.onNext(ErrorFrameFlyweight.encode(allocator, streamId, t));
  }

  private void handleRequestN(int streamId, ByteBuf frame) {
    final Subscription subscription = sendingSubscriptions.get(streamId);
    if (subscription != null) {
      int n = RequestNFrameFlyweight.requestN(frame);
      subscription.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
    }
  }
}
