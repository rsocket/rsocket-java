/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.micrometer.observation;

import io.micrometer.common.util.StringUtils;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.docs.DocumentedObservation;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.util.RSocketProxy;
import java.util.Iterator;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

/**
 * Tracing representation of a {@link RSocketProxy} for the requester.
 *
 * @author Marcin Grzejszczak
 * @author Oleh Dokuka
 * @since 3.1.0
 */
public class ObservationRequesterRSocketProxy extends RSocketProxy {

  private final ObservationRegistry observationRegistry;

  private RSocketRequesterObservationConvention observationConvention;

  public ObservationRequesterRSocketProxy(RSocket source, ObservationRegistry observationRegistry) {
    super(source);
    this.observationRegistry = observationRegistry;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return setObservation(
        super::fireAndForget,
        payload,
        FrameType.REQUEST_FNF,
        RSocketDocumentedObservation.RSOCKET_REQUESTER_FNF);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return setObservation(
        super::requestResponse,
        payload,
        FrameType.REQUEST_RESPONSE,
        RSocketDocumentedObservation.RSOCKET_REQUESTER_REQUEST_RESPONSE);
  }

  <T> Mono<T> setObservation(
      Function<Payload, Mono<T>> input,
      Payload payload,
      FrameType frameType,
      DocumentedObservation observation) {
    return Mono.deferContextual(
        contextView -> {
          if (contextView.hasKey(Observation.class)) {
            Observation parent = contextView.get(Observation.class);
            try (Observation.Scope scope = parent.openScope()) {
              return observe(input, payload, frameType, observation);
            }
          }
          return observe(input, payload, frameType, observation);
        });
  }

  private String route(Payload payload) {
    if (payload.hasMetadata()) {
      try {
        ByteBuf extracted =
            CompositeMetadataUtils.extract(
                payload.sliceMetadata(), WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.getString());
        final RoutingMetadata routingMetadata = new RoutingMetadata(extracted);
        final Iterator<String> iterator = routingMetadata.iterator();
        return iterator.next();
      } catch (Exception e) {

      }
    }
    return null;
  }

  private <T> Mono<T> observe(
      Function<Payload, Mono<T>> input,
      Payload payload,
      FrameType frameType,
      DocumentedObservation obs) {
    String route = route(payload);
    RSocketContext rSocketContext =
        new RSocketContext(
            payload, payload.sliceMetadata(), frameType, route, RSocketContext.Side.REQUESTER);
    Observation observation =
        obs.start(
            this.observationConvention,
            new DefaultRSocketRequesterObservationConvention(rSocketContext),
            rSocketContext,
            observationRegistry);
    setContextualName(frameType, route, observation);
    Payload newPayload = payload;
    if (rSocketContext.modifiedPayload != null) {
      newPayload = rSocketContext.modifiedPayload;
    }
    return input
        .apply(newPayload)
        .doOnError(observation::error)
        .doFinally(signalType -> observation.stop());
  }

  private Observation observation(ContextView contextView) {
    if (contextView.hasKey(Observation.class)) {
      return contextView.get(Observation.class);
    }
    return null;
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return Flux.deferContextual(
        contextView ->
            setObservation(
                super::requestStream,
                payload,
                contextView,
                FrameType.REQUEST_STREAM,
                RSocketDocumentedObservation.RSOCKET_REQUESTER_REQUEST_STREAM));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> inbound) {
    return Flux.from(inbound)
        .switchOnFirst(
            (firstSignal, flux) -> {
              final Payload firstPayload = firstSignal.get();
              if (firstPayload != null) {
                return setObservation(
                    p -> super.requestChannel(flux.skip(1).startWith(p)),
                    firstPayload,
                    firstSignal.getContextView(),
                    FrameType.REQUEST_CHANNEL,
                    RSocketDocumentedObservation.RSOCKET_REQUESTER_REQUEST_CHANNEL);
              }
              return flux;
            });
  }

  private Flux<Payload> setObservation(
      Function<Payload, Flux<Payload>> input,
      Payload payload,
      ContextView contextView,
      FrameType frameType,
      DocumentedObservation obs) {
    Observation parentObservation = observation(contextView);
    if (parentObservation == null) {
      return observationFlux(input, payload, frameType, obs);
    }
    try (Observation.Scope scope = parentObservation.openScope()) {
      return observationFlux(input, payload, frameType, obs);
    }
  }

  private Flux<Payload> observationFlux(
      Function<Payload, Flux<Payload>> input,
      Payload payload,
      FrameType frameType,
      DocumentedObservation obs) {
    return Flux.deferContextual(
        contextView -> {
          String route = route(payload);
          RSocketContext rSocketContext =
              new RSocketContext(
                  payload,
                  payload.sliceMetadata(),
                  frameType,
                  route,
                  RSocketContext.Side.REQUESTER);
          Observation newObservation =
              obs.start(
                  this.observationConvention,
                  new DefaultRSocketRequesterObservationConvention(rSocketContext),
                  rSocketContext,
                  this.observationRegistry);
          setContextualName(frameType, route, newObservation);
          return input
              .apply(rSocketContext.modifiedPayload)
              .doOnError(newObservation::error)
              .doFinally(signalType -> newObservation.stop());
        });
  }

  private void setContextualName(FrameType frameType, String route, Observation newObservation) {
    if (StringUtils.isNotBlank(route)) {
      newObservation.contextualName(frameType.name() + " " + route);
    } else {
      newObservation.contextualName(frameType.name());
    }
  }

  public void setObservationConvention(RSocketRequesterObservationConvention convention) {
    this.observationConvention = convention;
  }
}
