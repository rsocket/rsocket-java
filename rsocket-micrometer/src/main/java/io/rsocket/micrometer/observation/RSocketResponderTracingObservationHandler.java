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

import io.micrometer.observation.Observation;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.TracingObservationHandler;
import io.micrometer.tracing.internal.EncodingUtils;
import io.micrometer.tracing.propagation.Propagator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.rsocket.Payload;
import io.rsocket.frame.FrameType;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.TracingMetadata;
import io.rsocket.metadata.TracingMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;
import java.util.HashSet;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSocketResponderTracingObservationHandler
    implements TracingObservationHandler<RSocketContext> {

  private static final Logger log =
      LoggerFactory.getLogger(RSocketResponderTracingObservationHandler.class);

  private final Propagator propagator;

  private final Propagator.Getter<ByteBuf> getter;

  private final Tracer tracer;

  private final boolean isZipkinPropagationEnabled;

  public RSocketResponderTracingObservationHandler(
      Tracer tracer,
      Propagator propagator,
      Propagator.Getter<ByteBuf> getter,
      boolean isZipkinPropagationEnabled) {
    this.tracer = tracer;
    this.propagator = propagator;
    this.getter = getter;
    this.isZipkinPropagationEnabled = isZipkinPropagationEnabled;
  }

  @Override
  public void onStart(RSocketContext context) {
    Span handle = consumerSpanBuilder(context.payload, context.metadata, context.frameType);
    CompositeByteBuf bufs =
        PayloadUtils.cleanTracingMetadata(context.payload, new HashSet<>(propagator.fields()));
    context.modifiedPayload = PayloadUtils.payload(context.payload, bufs);
    getTracingContext(context).setSpan(handle);
  }

  @Override
  public void onError(RSocketContext context) {
    Throwable error = context.getError();
    if (error != null) {
      getRequiredSpan(context).error(error);
    }
  }

  @Override
  public void onStop(RSocketContext context) {
    Span span = getRequiredSpan(context);
    tagSpan(context, span);
    span.end();
  }

  @Override
  public boolean supportsContext(Observation.Context context) {
    return context instanceof RSocketContext
        && ((RSocketContext) context).side == RSocketContext.Side.RESPONDER;
  }

  @Override
  public Tracer getTracer() {
    return this.tracer;
  }

  private Span consumerSpanBuilder(Payload payload, ByteBuf headers, FrameType requestType) {
    Span.Builder consumerSpanBuilder = consumerSpanBuilder(payload, headers);
    log.debug("Extracted result from headers {}", consumerSpanBuilder);
    String name = "handle";
    if (payload.hasMetadata()) {
      try {
        final ByteBuf extract =
            CompositeMetadataUtils.extract(
                headers, WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.getString());
        if (extract != null) {
          final RoutingMetadata routingMetadata = new RoutingMetadata(extract);
          final Iterator<String> iterator = routingMetadata.iterator();
          name = requestType.name() + " " + iterator.next();
        }
      } catch (Exception e) {

      }
    }
    return consumerSpanBuilder.kind(Span.Kind.CONSUMER).name(name).start();
  }

  private Span.Builder consumerSpanBuilder(Payload payload, ByteBuf headers) {
    if (this.isZipkinPropagationEnabled && payload.hasMetadata()) {
      try {
        ByteBuf extract =
            CompositeMetadataUtils.extract(
                headers, WellKnownMimeType.MESSAGE_RSOCKET_TRACING_ZIPKIN.getString());
        if (extract != null) {
          TracingMetadata tracingMetadata = TracingMetadataCodec.decode(extract);
          Span.Builder builder = this.tracer.spanBuilder();
          String traceId = EncodingUtils.fromLong(tracingMetadata.traceId());
          long traceIdHigh = tracingMetadata.traceIdHigh();
          if (traceIdHigh != 0L) {
            // ExtendedTraceId
            traceId = EncodingUtils.fromLong(traceIdHigh) + traceId;
          }
          TraceContext.Builder parentBuilder =
              this.tracer
                  .traceContextBuilder()
                  .sampled(tracingMetadata.isDebug() || tracingMetadata.isSampled())
                  .traceId(traceId)
                  .spanId(EncodingUtils.fromLong(tracingMetadata.spanId()))
                  .parentId(EncodingUtils.fromLong(tracingMetadata.parentId()));
          return builder.setParent(parentBuilder.build());
        } else {
          return this.propagator.extract(headers, this.getter);
        }
      } catch (Exception e) {

      }
    }
    return this.propagator.extract(headers, this.getter);
  }
}
