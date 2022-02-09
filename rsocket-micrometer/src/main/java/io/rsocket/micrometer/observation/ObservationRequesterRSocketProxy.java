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

import java.util.Iterator;
import java.util.function.Function;

import io.micrometer.api.instrument.Tag;
import io.micrometer.api.instrument.docs.DocumentedObservation;
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

	public ObservationRequesterRSocketProxy(RSocket source, ObservationRegistry observationRegistry) {
		super(source);
		this.observationRegistry = observationRegistry;
	}

	@Override
	public Mono<Void> fireAndForget(Payload payload) {
		return setObservation(super::fireAndForget, payload, FrameType.REQUEST_FNF, RSocketObservation.RSOCKET_REQUESTER_FNF);
	}

	@Override
	public Mono<Payload> requestResponse(Payload payload) {
		return setObservation(super::requestResponse, payload, FrameType.REQUEST_RESPONSE, RSocketObservation.RSOCKET_REQUESTER_REQUEST_RESPONSE);
	}

	<T> Mono<T> setObservation(Function<Payload, Mono<T>> input, Payload payload, FrameType frameType, DocumentedObservation observation) {
		return Mono.deferContextual(contextView -> {
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
				ByteBuf extracted = CompositeMetadataUtils.extract(payload.sliceMetadata(),
						WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.getString());
				final RoutingMetadata routingMetadata = new RoutingMetadata(extracted);
				final Iterator<String> iterator = routingMetadata.iterator();
				return iterator.next();
			} catch (Exception e) {

			}
		}
		return null;
	}

	private <T> Mono<T> observe(Function<Payload, Mono<T>> input, Payload payload, FrameType frameType, DocumentedObservation obs) {
		RSocketContext rSocketContext = new RSocketContext(payload, payload.sliceMetadata(), frameType, RSocketContext.Side.REQUESTER);
		Observation observation = Observation.start(obs.getName(), rSocketContext, observationRegistry)
				.lowCardinalityTag(RSocketObservation.RequesterTags.REQUEST_TYPE.getKey(), frameType.name());
		// TODO: Add low cardinality tag for content-type
		addRouteRelatedElements(payload, frameType, observation);
		Payload newPayload = payload;
		if (rSocketContext.modifiedPayload != null) {
			newPayload = rSocketContext.modifiedPayload;
		}
		return input.apply(newPayload).doOnError(observation::error).doFinally(signalType -> observation.stop());
	}

	private Observation observation(ContextView contextView) {
		if (contextView.hasKey(Observation.class)) {
			return contextView.get(Observation.class);
		}
		return null;
	}

	@Override
	public Flux<Payload> requestStream(Payload payload) {
		return Flux.deferContextual(contextView -> setObservation(super::requestStream, payload, contextView, FrameType.REQUEST_STREAM, RSocketObservation.RSOCKET_REQUESTER_REQUEST_STREAM));
	}

	@Override
	public Flux<Payload> requestChannel(Publisher<Payload> inbound) {
		return Flux.from(inbound).switchOnFirst((firstSignal, flux) -> {
			final Payload firstPayload = firstSignal.get();
			if (firstPayload != null) {
				return setObservation(p -> super.requestChannel(flux.skip(1).startWith(p)), firstPayload,
						firstSignal.getContextView(), FrameType.REQUEST_CHANNEL, RSocketObservation.RSOCKET_REQUESTER_REQUEST_CHANNEL);
			}
			return flux;
		});
	}

	private Flux<Payload> setObservation(Function<Payload, Flux<Payload>> input, Payload payload, ContextView contextView, FrameType frameType, DocumentedObservation obs) {
		Observation parentObservation = observation(contextView);
		if (parentObservation == null) {
			return observationFlux(input, payload, frameType, obs);
		}
		try (Observation.Scope scope = parentObservation.openScope()) {
			return observationFlux(input, payload, frameType, obs);
		}
	}

	private Flux<Payload> observationFlux(Function<Payload, Flux<Payload>> input, Payload payload, FrameType frameType, DocumentedObservation obs) {
		return Flux.deferContextual(contextView -> {
			RSocketContext rSocketContext = new RSocketContext(payload, payload.sliceMetadata(), frameType, RSocketContext.Side.REQUESTER);
			Observation newObservation = Observation.start(obs.getName(), rSocketContext, this.observationRegistry)
					.lowCardinalityTag(RSocketObservation.ResponderTags.REQUEST_TYPE.getKey(), frameType.name());
			addRouteRelatedElements(payload, frameType, newObservation);
			return input.apply(rSocketContext.modifiedPayload).doOnError(newObservation::error).doFinally(signalType -> newObservation.stop());
		});
	}

	private void addRouteRelatedElements(Payload payload, FrameType frameType, Observation newObservation) {
		String route = route(payload);
		if (StringUtils.isNotBlank(route)) {
			newObservation.contextualName(frameType.name() + " " + route);
			newObservation.lowCardinalityTag(Tag.of(RSocketObservation.RequesterTags.ROUTE.getKey(), route));
		} else {
			newObservation.contextualName(frameType.name());
		}
	}

}
