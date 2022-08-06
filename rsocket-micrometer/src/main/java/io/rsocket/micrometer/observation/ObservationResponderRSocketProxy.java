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

  private RSocketResponderObservationConvention observationConvention;

  public ObservationResponderRSocketProxy(RSocket source, ObservationRegistry observationRegistry) {
    super(source);
    this.observationRegistry = observationRegistry;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    // called on Netty EventLoop
    // there can't be observation in thread local here
    ByteBuf sliceMetadata = payload.sliceMetadata();
    String route = route(payload, sliceMetadata);
    RSocketContext rSocketContext =
        new RSocketContext(
            payload,
            payload.sliceMetadata(),
            FrameType.REQUEST_FNF,
            route,
            RSocketContext.Side.RESPONDER);
    Observation newObservation =
        startObservation(RSocketDocumentedObservation.RSOCKET_RESPONDER_FNF, rSocketContext);
    return super.fireAndForget(rSocketContext.modifiedPayload)
        .doOnError(newObservation::error)
        .doFinally(signalType -> newObservation.stop());
  }

  private Observation startObservation(
      RSocketDocumentedObservation observation, RSocketContext rSocketContext) {
    return observation.start(
        this.observationConvention,
        new DefaultRSocketResponderObservationConvention(rSocketContext),
        rSocketContext,
        this.observationRegistry);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    ByteBuf sliceMetadata = payload.sliceMetadata();
    String route = route(payload, sliceMetadata);
    RSocketContext rSocketContext =
        new RSocketContext(
            payload,
            payload.sliceMetadata(),
            FrameType.REQUEST_RESPONSE,
            route,
            RSocketContext.Side.RESPONDER);
    Observation newObservation =
        startObservation(
            RSocketDocumentedObservation.RSOCKET_RESPONDER_REQUEST_RESPONSE, rSocketContext);
    return super.requestResponse(rSocketContext.modifiedPayload)
        .doOnError(newObservation::error)
        .doFinally(signalType -> newObservation.stop());
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    ByteBuf sliceMetadata = payload.sliceMetadata();
    String route = route(payload, sliceMetadata);
    RSocketContext rSocketContext =
        new RSocketContext(
            payload, sliceMetadata, FrameType.REQUEST_STREAM, route, RSocketContext.Side.RESPONDER);
    Observation newObservation =
        startObservation(
            RSocketDocumentedObservation.RSOCKET_RESPONDER_REQUEST_STREAM, rSocketContext);
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
                ByteBuf sliceMetadata = firstPayload.sliceMetadata();
                String route = route(firstPayload, sliceMetadata);
                RSocketContext rSocketContext =
                    new RSocketContext(
                        firstPayload,
                        firstPayload.sliceMetadata(),
                        FrameType.REQUEST_CHANNEL,
                        route,
                        RSocketContext.Side.RESPONDER);
                Observation newObservation =
                    startObservation(
                        RSocketDocumentedObservation.RSOCKET_RESPONDER_REQUEST_CHANNEL,
                        rSocketContext);
                if (StringUtils.isNotBlank(route)) {
                  newObservation.contextualName(rSocketContext.frameType.name() + " " + route);
                }
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

  public void setObservationConvention(RSocketResponderObservationConvention convention) {
    this.observationConvention = convention;
  }
}
