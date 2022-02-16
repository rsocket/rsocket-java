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

import io.micrometer.api.instrument.Tag;
import io.micrometer.api.instrument.observation.Observation;
import io.micrometer.api.instrument.observation.ObservationRegistry;
import io.micrometer.api.instrument.util.StringUtils;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.util.RSocketProxy;
import java.util.Iterator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tracing representation of a {@link RSocketProxy} for the responder.
 *
 * @author Marcin Grzejszczak
 * @author Oleh Dokuka
 * @since 3.1.0
 */
public class ObservationResponderRSocketProxy extends RSocketProxy {

  private final ObservationRegistry observationRegistry;

  public ObservationResponderRSocketProxy(RSocket source, ObservationRegistry observationRegistry) {
    super(source);
    this.observationRegistry = observationRegistry;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    // called on Netty EventLoop
    // there can't be trace context in thread local here
    RSocketContext rSocketContext =
        new RSocketContext(
            payload, payload.sliceMetadata(), FrameType.REQUEST_FNF, RSocketContext.Side.RESPONDER);
    Observation newObservation =
        Observation.start(
                RSocketObservation.RSOCKET_RESPONDER_FNF.getName(),
                rSocketContext,
                this.observationRegistry)
            .lowCardinalityTag(
                RSocketObservation.ResponderTags.REQUEST_TYPE.getKey(),
                FrameType.REQUEST_FNF.name());
    addRouteRelatedElements(
        newObservation, FrameType.REQUEST_FNF, route(payload, payload.sliceMetadata()));
    return super.fireAndForget(rSocketContext.modifiedPayload)
        .doOnError(newObservation::error)
        .doFinally(signalType -> newObservation.stop());
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    RSocketContext rSocketContext =
        new RSocketContext(
            payload,
            payload.sliceMetadata(),
            FrameType.REQUEST_RESPONSE,
            RSocketContext.Side.RESPONDER);
    Observation newObservation =
        Observation.start(
                RSocketObservation.RSOCKET_RESPONDER_REQUEST_RESPONSE.getName(),
                rSocketContext,
                this.observationRegistry)
            .lowCardinalityTag(
                RSocketObservation.ResponderTags.REQUEST_TYPE.getKey(),
                FrameType.REQUEST_RESPONSE.name());
    addRouteRelatedElements(
        newObservation, FrameType.REQUEST_RESPONSE, route(payload, payload.sliceMetadata()));
    return super.requestResponse(rSocketContext.modifiedPayload)
        .doOnError(newObservation::error)
        .doFinally(signalType -> newObservation.stop());
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    RSocketContext rSocketContext =
        new RSocketContext(
            payload,
            payload.sliceMetadata(),
            FrameType.REQUEST_STREAM,
            RSocketContext.Side.RESPONDER);
    Observation newObservation =
        Observation.start(
                RSocketObservation.RSOCKET_RESPONDER_REQUEST_STREAM.getName(),
                rSocketContext,
                this.observationRegistry)
            .lowCardinalityTag(
                RSocketObservation.ResponderTags.REQUEST_TYPE.getKey(),
                FrameType.REQUEST_STREAM.name());
    addRouteRelatedElements(
        newObservation, FrameType.REQUEST_STREAM, route(payload, payload.sliceMetadata()));
    return super.requestStream(rSocketContext.modifiedPayload)
        .doOnError(newObservation::error)
        .doFinally(signalType -> newObservation.stop());
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return Flux.from(payloads)
        .switchOnFirst(
            (firstSignal, flux) -> {
              final Payload firstPayload = firstSignal.get();
              if (firstPayload != null) {
                RSocketContext rSocketContext =
                    new RSocketContext(
                        firstPayload,
                        firstPayload.sliceMetadata(),
                        FrameType.REQUEST_CHANNEL,
                        RSocketContext.Side.RESPONDER);
                Observation newObservation =
                    Observation.start(
                            RSocketObservation.RSOCKET_RESPONDER_REQUEST_CHANNEL.getName(),
                            rSocketContext,
                            this.observationRegistry)
                        .lowCardinalityTag(
                            RSocketObservation.ResponderTags.REQUEST_TYPE.getKey(),
                            FrameType.REQUEST_CHANNEL.name());
                ;
                addRouteRelatedElements(
                    newObservation,
                    FrameType.REQUEST_CHANNEL,
                    route(firstPayload, firstPayload.sliceMetadata()));
                return super.requestChannel(flux.skip(1).startWith(rSocketContext.modifiedPayload))
                    .doOnError(newObservation::error)
                    .doFinally(signalType -> newObservation.stop());
              }
              return flux;
            });
  }

  private String route(Payload payload, ByteBuf headers) {
    if (payload.hasMetadata()) {
      try {
        final ByteBuf extract =
            CompositeMetadataUtils.extract(
                headers, WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.getString());
        if (extract != null) {
          final RoutingMetadata routingMetadata = new RoutingMetadata(extract);
          final Iterator<String> iterator = routingMetadata.iterator();
          return iterator.next();
        }
      } catch (Exception e) {

      }
    }
    return null;
  }

  private void addRouteRelatedElements(Observation observation, FrameType frameType, String route) {
    if (StringUtils.isBlank(route)) {
      return;
    }
    observation.contextualName(frameType.name() + " " + route);
    observation.lowCardinalityTag(Tag.of(RSocketObservation.ResponderTags.ROUTE.getKey(), route));
  }
}
