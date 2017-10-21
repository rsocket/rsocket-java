/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket;

import static io.rsocket.Frame.Request.initialRequestN;
import static io.rsocket.frame.FrameHeaderFlyweight.FLAGS_C;
import static io.rsocket.frame.FrameHeaderFlyweight.FLAGS_M;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.collection.IntObjectHashMap;
import io.rsocket.exceptions.ApplicationException;
import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.util.PayloadImpl;
import java.util.Collection;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.*;

/** Server side RSocket. Receives {@link Frame}s from a {@link RSocketClient} */
class RSocketServer implements RSocket {

  private final DuplexConnection connection;
  private final RSocket requestHandler;
  private final Consumer<Throwable> errorConsumer;

  private final IntObjectHashMap<Subscription> sendingSubscriptions;
  private final IntObjectHashMap<UnicastProcessor<Payload>> channelProcessors;

  private final FluxProcessor<Frame, Frame> sendProcessor;
  private Disposable receiveDisposable;

  RSocketServer(
      DuplexConnection connection, RSocket requestHandler, Consumer<Throwable> errorConsumer) {
    this.connection = connection;
    this.requestHandler = requestHandler;
    this.errorConsumer = errorConsumer;
    this.sendingSubscriptions = new IntObjectHashMap<>();
    this.channelProcessors = new IntObjectHashMap<>();

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    // connections
    this.sendProcessor = EmitterProcessor.<Frame>create().serialize();

    connection
        .send(sendProcessor)
        .doOnError(this::handleSendProcessorError)
        .doFinally(this::handleSendProcessorCancel)
        .subscribe();

    this.receiveDisposable =
        connection
            .receive()
            .flatMapSequential(this::handleFrame)
            .doOnError(errorConsumer)
            .then()
            .subscribe();

    this.connection
        .onClose()
        .doOnError(errorConsumer)
        .doFinally(
            s -> {
              cleanup();
              receiveDisposable.dispose();
            })
        .subscribe();
  }

  private void handleSendProcessorError(Throwable t) {
    Collection<Subscription> values;
    Collection<UnicastProcessor<Payload>> values1;
    synchronized (RSocketServer.this) {
      values = sendingSubscriptions.values();
      values1 = channelProcessors.values();
    }

    for (Subscription subscription : values) {
      try {
        subscription.cancel();
      } catch (Throwable e) {
        errorConsumer.accept(e);
      }
    }

    for (UnicastProcessor subscription : values1) {
      try {
        subscription.cancel();
      } catch (Throwable e) {
        errorConsumer.accept(e);
      }
    }
  }

  private void handleSendProcessorCancel(SignalType t) {
    if (SignalType.ON_ERROR == t) {
      return;
    }
    Collection<Subscription> values;
    Collection<UnicastProcessor<Payload>> values1;
    synchronized (RSocketServer.this) {
      values = sendingSubscriptions.values();
      values1 = channelProcessors.values();
    }

    for (Subscription subscription : values) {
      try {
        subscription.cancel();
      } catch (Throwable e) {
        errorConsumer.accept(e);
      }
    }

    for (UnicastProcessor subscription : values1) {
      try {
        subscription.cancel();
      } catch (Throwable e) {
        errorConsumer.accept(e);
      }
    }
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
  public Mono<Void> close() {
    return connection.close();
  }

  @Override
  public Mono<Void> onClose() {
    return connection.onClose();
  }

  private void cleanup() {
    cleanUpSendingSubscriptions();
    cleanUpChannelProcessors();

    requestHandler.close().subscribe();
  }

  private synchronized void cleanUpSendingSubscriptions() {
    sendingSubscriptions.values().forEach(Subscription::cancel);
    sendingSubscriptions.clear();
  }

  private synchronized void cleanUpChannelProcessors() {
    channelProcessors.values().forEach(Subscription::cancel);
    channelProcessors.clear();
  }

  private Mono<Void> handleFrame(Frame frame) {
    try {
      int streamId = frame.getStreamId();
      Subscriber<Payload> receiver;
      switch (frame.getType()) {
        case FIRE_AND_FORGET:
          return handleFireAndForget(streamId, fireAndForget(new PayloadImpl(frame)));
        case REQUEST_RESPONSE:
          return handleRequestResponse(streamId, requestResponse(new PayloadImpl(frame)));
        case CANCEL:
          return handleCancelFrame(streamId);
        case KEEPALIVE:
          return handleKeepAliveFrame(frame);
        case REQUEST_N:
          return handleRequestN(streamId, frame);
        case REQUEST_STREAM:
          return handleStream(
              streamId, requestStream(new PayloadImpl(frame)), initialRequestN(frame));
        case REQUEST_CHANNEL:
          return handleChannel(streamId, frame);
        case PAYLOAD:
          // TODO: Hook in receiving socket.
          return Mono.empty();
        case METADATA_PUSH:
          return metadataPush(new PayloadImpl(frame));
        case LEASE:
          // Lease must not be received here as this is the server end of the socket which sends
          // leases.
          return Mono.empty();
        case NEXT:
          receiver = getChannelProcessor(streamId);
          if (receiver != null) {
            receiver.onNext(new PayloadImpl(frame));
          }
          return Mono.empty();
        case COMPLETE:
          receiver = getChannelProcessor(streamId);
          if (receiver != null) {
            receiver.onComplete();
          }
          return Mono.empty();
        case ERROR:
          receiver = getChannelProcessor(streamId);
          if (receiver != null) {
            receiver.onError(new ApplicationException(Frame.Error.message(frame)));
          }
          return Mono.empty();
        case NEXT_COMPLETE:
          receiver = getChannelProcessor(streamId);
          if (receiver != null) {
            receiver.onNext(new PayloadImpl(frame));
            receiver.onComplete();
          }

          return Mono.empty();

        case SETUP:
          return handleError(
              streamId, new IllegalStateException("Setup frame received post setup."));
        default:
          return handleError(
              streamId,
              new IllegalStateException(
                  "ServerRSocket: Unexpected frame type: " + frame.getType()));
      }
    } finally {
      frame.release();
    }
  }

  private Mono<Void> handleFireAndForget(int streamId, Mono<Void> result) {
    return result
        .doOnSubscribe(subscription -> addSubscription(streamId, subscription))
        .doOnError(errorConsumer)
        .doFinally(signalType -> removeSubscription(streamId))
        .ignoreElement();
  }

  private Mono<Void> handleRequestResponse(int streamId, Mono<Payload> response) {
    return response
        .doOnSubscribe(subscription -> addSubscription(streamId, subscription))
        .map(
            payload -> {
              int flags = FLAGS_C;
              if (payload.hasMetadata()) {
                flags = Frame.setFlag(flags, FLAGS_M);
              }
              return Frame.PayloadFrame.from(streamId, FrameType.NEXT_COMPLETE, payload, flags);
            })
        .doOnError(errorConsumer)
        .onErrorResume(t -> Mono.just(Frame.Error.from(streamId, t)))
        .doOnNext(sendProcessor::onNext)
        .doFinally(signalType -> removeSubscription(streamId))
        .then();
  }

  private Mono<Void> handleStream(int streamId, Flux<Payload> response, int initialRequestN) {
    response
        .map(payload -> Frame.PayloadFrame.from(streamId, FrameType.NEXT, payload))
        .transform(
            frameFlux -> {
              LimitableRequestPublisher<Frame> frames = LimitableRequestPublisher.wrap(frameFlux);
              synchronized (RSocketServer.this) {
                sendingSubscriptions.put(streamId, frames);
              }
              frames.increaseRequestLimit(initialRequestN);
              return frames;
            })
        .concatWith(Mono.just(Frame.PayloadFrame.from(streamId, FrameType.COMPLETE)))
        .onErrorResume(t -> Mono.just(Frame.Error.from(streamId, t)))
        .doOnNext(sendProcessor::onNext)
        .doFinally(signalType -> removeSubscription(streamId))
        .subscribe();

    return Mono.empty();
  }

  private Mono<Void> handleChannel(int streamId, Frame firstFrame) {
    UnicastProcessor<Payload> frames = UnicastProcessor.create();
    addChannelProcessor(streamId, frames);

    Flux<Payload> payloads =
        frames
            .doOnCancel(
                () -> {
                  sendProcessor.onNext(Frame.Cancel.from(streamId));
                })
            .doOnError(
                t -> {
                  sendProcessor.onNext(Frame.Error.from(streamId, t));
                })
            .doOnRequest(
                l -> {
                  sendProcessor.onNext(Frame.RequestN.from(streamId, l));
                })
            .doFinally(signalType -> removeChannelProcessor(streamId));

    // not chained, as the payload should be enqueued in the Unicast processor before this method
    // returns
    // and any later payload can be processed
    frames.onNext(new PayloadImpl(firstFrame));

    return handleStream(streamId, requestChannel(payloads), initialRequestN(firstFrame));
  }

  private Mono<Void> handleKeepAliveFrame(Frame frame) {
    return Mono.fromRunnable(
        () -> {
          if (Frame.Keepalive.hasRespondFlag(frame)) {
            ByteBuf data = Unpooled.wrappedBuffer(frame.getData());
            sendProcessor.onNext(Frame.Keepalive.from(data, false));
          }
        });
  }

  private Mono<Void> handleCancelFrame(int streamId) {
    return Mono.fromRunnable(
        () -> {
          Subscription subscription;
          synchronized (this) {
            subscription = sendingSubscriptions.remove(streamId);
          }

          if (subscription != null) {
            subscription.cancel();
          }
        });
  }

  private Mono<Void> handleError(int streamId, Throwable t) {
    return Mono.fromRunnable(
        () -> {
          errorConsumer.accept(t);
          sendProcessor.onNext(Frame.Error.from(streamId, t));
        });
  }

  private Mono<Void> handleRequestN(int streamId, Frame frame) {
    final Subscription subscription = getSubscription(streamId);
    if (subscription != null) {
      int n = Frame.RequestN.requestN(frame);
      subscription.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
    }
    return Mono.empty();
  }

  private synchronized void addSubscription(int streamId, Subscription subscription) {
    sendingSubscriptions.put(streamId, subscription);
  }

  private synchronized @Nullable Subscription getSubscription(int streamId) {
    return sendingSubscriptions.get(streamId);
  }

  private synchronized void removeSubscription(int streamId) {
    sendingSubscriptions.remove(streamId);
  }

  private synchronized void addChannelProcessor(int streamId, UnicastProcessor<Payload> processor) {
    channelProcessors.put(streamId, processor);
  }

  private synchronized @Nullable UnicastProcessor<Payload> getChannelProcessor(int streamId) {
    return channelProcessors.get(streamId);
  }

  private synchronized void removeChannelProcessor(int streamId) {
    channelProcessors.remove(streamId);
  }
}
