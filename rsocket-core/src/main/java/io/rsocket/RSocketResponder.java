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
import io.netty.util.collection.IntObjectMap;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.frame.*;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.internal.SynchronizedIntObjectHashMap;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.lease.ResponderLeaseHandler;
import java.util.function.Consumer;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.*;

/** Responder side of RSocket. Receives {@link ByteBuf}s from a peer's {@link RSocketRequester} */
class RSocketResponder implements ResponderRSocket {

  private final DuplexConnection connection;
  private final RSocket requestHandler;
  private final ResponderRSocket responderRSocket;
  private final PayloadDecoder payloadDecoder;
  private final Consumer<Throwable> errorConsumer;
  private final ResponderLeaseHandler leaseHandler;

  private final IntObjectMap<LimitableRequestPublisher> sendingLimitableSubscriptions;
  private final IntObjectMap<Subscription> sendingSubscriptions;
  private final IntObjectMap<Processor<Payload, Payload>> channelProcessors;

  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final ByteBufAllocator allocator;

  RSocketResponder(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      ResponderLeaseHandler leaseHandler) {
    this.allocator = allocator;
    this.connection = connection;

    this.requestHandler = requestHandler;
    this.responderRSocket =
        (requestHandler instanceof ResponderRSocket) ? (ResponderRSocket) requestHandler : null;

    this.payloadDecoder = payloadDecoder;
    this.errorConsumer = errorConsumer;
    this.leaseHandler = leaseHandler;
    this.sendingLimitableSubscriptions = new SynchronizedIntObjectHashMap<>();
    this.sendingSubscriptions = new SynchronizedIntObjectHashMap<>();
    this.channelProcessors = new SynchronizedIntObjectHashMap<>();

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    // connections
    this.sendProcessor = new UnboundedProcessor<>();

    connection
        .send(sendProcessor)
        .doFinally(this::handleSendProcessorCancel)
        .subscribe(null, this::handleSendProcessorError);

    Disposable receiveDisposable = connection.receive().subscribe(this::handleFrame, errorConsumer);
    Disposable sendLeaseDisposable = leaseHandler.send(sendProcessor::onNext);

    this.connection
        .onClose()
        .doFinally(
            s -> {
              cleanup();
              receiveDisposable.dispose();
              sendLeaseDisposable.dispose();
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

  @Override
  public Flux<Payload> requestChannel(Payload payload, Publisher<Payload> payloads) {
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
        case REQUEST_N:
          handleRequestN(streamId, frame);
          break;
        case REQUEST_STREAM:
          int streamInitialRequestN = RequestStreamFrameFlyweight.initialRequestN(frame);
          Payload streamPayload = payloadDecoder.apply(frame);
          handleStream(streamId, requestStream(streamPayload), streamInitialRequestN);
          break;
        case REQUEST_CHANNEL:
          int channelInitialRequestN = RequestChannelFrameFlyweight.initialRequestN(frame);
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
                  LimitableRequestPublisher.wrap(frameFlux);
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

  private void handleMetadataPush(Mono<Void> result) {
    result.subscribe(
        new BaseSubscriber<Void>() {
          @Override
          protected void hookOnSubscribe(Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          protected void hookOnError(Throwable throwable) {
            errorConsumer.accept(throwable);
          }
        });
  }

  private void handleCancelFrame(int streamId) {
    Subscription subscription = sendingSubscriptions.remove(streamId);

    if (subscription == null) {
      subscription = sendingLimitableSubscriptions.remove(streamId);
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
