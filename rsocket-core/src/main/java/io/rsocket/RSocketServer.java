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

import static io.rsocket.Frame.Request.initialRequestN;
import static io.rsocket.frame.FrameHeaderFlyweight.FLAGS_C;
import static io.rsocket.frame.FrameHeaderFlyweight.FLAGS_M;

import io.netty.util.collection.IntObjectHashMap;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.framing.FrameType;
import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.internal.UnboundedProcessor;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.UnicastProcessor;

/** Server side RSocket. Receives {@link Frame}s from a {@link RSocketClient} */
class RSocketServer implements ResponderRSocket {

  private final DuplexConnection connection;
  private final RSocket requestHandler;
  private final ResponderRSocket responderRSocket;
  private final Function<Frame, ? extends Payload> frameDecoder;
  private final Consumer<Throwable> errorConsumer;

  private final Map<Integer, Subscription> sendingSubscriptions;
  private final Map<Integer, Processor<Payload, Payload>> channelProcessors;

  private final UnboundedProcessor<Frame> sendProcessor;
  private KeepAliveHandler keepAliveHandler;

  /*client responder*/
  RSocketServer(
      DuplexConnection connection,
      RSocket requestHandler,
      Function<Frame, ? extends Payload> frameDecoder,
      Consumer<Throwable> errorConsumer) {
    this(connection, requestHandler, frameDecoder, errorConsumer, 0, 0);
  }

  /*server responder*/
  RSocketServer(
      DuplexConnection connection,
      RSocket requestHandler,
      Function<Frame, ? extends Payload> frameDecoder,
      Consumer<Throwable> errorConsumer,
      long tickPeriod,
      long ackTimeout) {

    this.requestHandler = requestHandler;
    this.responderRSocket =
        (requestHandler instanceof ResponderRSocket) ? (ResponderRSocket) requestHandler : null;

    this.connection = connection;
    this.frameDecoder = frameDecoder;
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

    if (tickPeriod != 0) {
      keepAliveHandler =
          KeepAliveHandler.ofServer(new KeepAliveHandler.KeepAlive(tickPeriod, ackTimeout));

      keepAliveHandler
          .timeout()
          .subscribe(
              keepAlive -> {
                String message =
                    String.format("No keep-alive acks for %d ms", keepAlive.getTimeoutMillis());
                errorConsumer.accept(new ConnectionErrorException(message));
                connection.dispose();
              });
      keepAliveHandler.send().subscribe(sendProcessor::onNext);
    } else {
      keepAliveHandler = null;
    }
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
    if (keepAliveHandler != null) {
      keepAliveHandler.dispose();
    }
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

  private void handleFrame(Frame frame) {
    try {
      int streamId = frame.getStreamId();
      Subscriber<Payload> receiver;
      switch (frame.getType()) {
        case REQUEST_FNF:
          handleFireAndForget(streamId, fireAndForget(frameDecoder.apply(frame)));
          break;
        case REQUEST_RESPONSE:
          handleRequestResponse(streamId, requestResponse(frameDecoder.apply(frame)));
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
          handleStream(streamId, requestStream(frameDecoder.apply(frame)), initialRequestN(frame));
          break;
        case REQUEST_CHANNEL:
          handleChannel(streamId, frameDecoder.apply(frame), initialRequestN(frame));
          break;
        case METADATA_PUSH:
          metadataPush(frameDecoder.apply(frame));
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
            receiver.onNext(frameDecoder.apply(frame));
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
            receiver.onError(new ApplicationErrorException(Frame.Error.message(frame)));
          }
          break;
        case NEXT_COMPLETE:
          receiver = channelProcessors.get(streamId);
          if (receiver != null) {
            receiver.onNext(frameDecoder.apply(frame));
            receiver.onComplete();
          }
          break;
        case SETUP:
          handleError(streamId, new IllegalStateException("Setup frame received post setup."));
          break;
        default:
          handleError(
              streamId,
              new IllegalStateException(
                  "ServerRSocket: Unexpected frame type: " + frame.getType()));
          break;
      }
    } finally {
      frame.release();
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
              int flags = FLAGS_C;
              if (payload.hasMetadata()) {
                flags = Frame.setFlag(flags, FLAGS_M);
              }
              final Frame frame =
                  Frame.PayloadFrame.from(streamId, FrameType.NEXT_COMPLETE, payload, flags);
              payload.release();
              return frame;
            })
        .switchIfEmpty(
            Mono.fromCallable(() -> Frame.PayloadFrame.from(streamId, FrameType.COMPLETE)))
        .doFinally(signalType -> sendingSubscriptions.remove(streamId))
        .subscribe(sendProcessor::onNext, t -> handleError(streamId, t));
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
              final Frame frame = Frame.PayloadFrame.from(streamId, FrameType.NEXT, payload);
              payload.release();
              sendProcessor.onNext(frame);
            },
            t -> handleError(streamId, t),
            () -> {
              final Frame frame = Frame.PayloadFrame.from(streamId, FrameType.COMPLETE);
              sendProcessor.onNext(frame);
            });
  }

  private void handleChannel(int streamId, Payload payload, int initialRequestN) {
    UnicastProcessor<Payload> frames = UnicastProcessor.create();
    channelProcessors.put(streamId, frames);

    Flux<Payload> payloads =
        frames
            .doOnCancel(() -> sendProcessor.onNext(Frame.Cancel.from(streamId)))
            .doOnError(t -> sendProcessor.onNext(Frame.Error.from(streamId, t)))
            .doOnRequest(l -> sendProcessor.onNext(Frame.RequestN.from(streamId, l)))
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

  private void handleKeepAliveFrame(Frame frame) {
    if (keepAliveHandler != null) {
      keepAliveHandler.receive(frame);
    }
  }

  private void handleCancelFrame(int streamId) {
    Subscription subscription = sendingSubscriptions.remove(streamId);
    if (subscription != null) {
      subscription.cancel();
    }
  }

  private void handleError(int streamId, Throwable t) {
    errorConsumer.accept(t);
    sendProcessor.onNext(Frame.Error.from(streamId, t));
  }

  private void handleRequestN(int streamId, Frame frame) {
    final Subscription subscription = sendingSubscriptions.get(streamId);
    if (subscription != null) {
      int n = Frame.RequestN.requestN(frame);
      subscription.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
    }
  }
}
