package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCounted;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TracingMetadataCodecTest {

  private static Stream<TracingMetadataCodec.Flags> flags() {
    return Stream.of(TracingMetadataCodec.Flags.values());
  }

  @ParameterizedTest
  @MethodSource("flags")
  public void shouldEncodeEmptyTrace(TracingMetadataCodec.Flags expectedFlag) {
    LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    ByteBuf byteBuf = TracingMetadataCodec.encodeEmpty(allocator, expectedFlag);

    TracingMetadata tracingMetadata = TracingMetadataCodec.decode(byteBuf);

    Assertions.assertThat(tracingMetadata)
        .matches(TracingMetadata::isEmpty)
        .matches(
            tm -> {
              switch (expectedFlag) {
                case UNDECIDED:
                  return !tm.isDecided();
                case NOT_SAMPLE:
                  return tm.isDecided() && !tm.isSampled();
                case SAMPLE:
                  return tm.isDecided() && tm.isSampled();
                case DEBUG:
                  return tm.isDecided() && tm.isDebug();
              }
              return false;
            });
    Assertions.assertThat(byteBuf).matches(ReferenceCounted::release);
    allocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("flags")
  public void shouldEncodeTrace64WithParent(TracingMetadataCodec.Flags expectedFlag) {
    long traceId = ThreadLocalRandom.current().nextLong();
    long spanId = ThreadLocalRandom.current().nextLong();
    long parentId = ThreadLocalRandom.current().nextLong();
    LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    ByteBuf byteBuf =
        TracingMetadataCodec.encode64(allocator, traceId, spanId, parentId, expectedFlag);

    TracingMetadata tracingMetadata = TracingMetadataCodec.decode(byteBuf);

    Assertions.assertThat(tracingMetadata)
        .matches(metadata -> !metadata.isEmpty())
        .matches(tm -> tm.traceIdHigh() == 0)
        .matches(tm -> tm.traceId() == traceId)
        .matches(tm -> tm.spanId() == spanId)
        .matches(tm -> tm.hasParent())
        .matches(tm -> tm.parentId() == parentId)
        .matches(
            tm -> {
              switch (expectedFlag) {
                case UNDECIDED:
                  return !tm.isDecided();
                case NOT_SAMPLE:
                  return tm.isDecided() && !tm.isSampled();
                case SAMPLE:
                  return tm.isDecided() && tm.isSampled();
                case DEBUG:
                  return tm.isDecided() && tm.isDebug();
              }
              return false;
            });
    Assertions.assertThat(byteBuf).matches(ReferenceCounted::release);
    allocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("flags")
  public void shouldEncodeTrace64(TracingMetadataCodec.Flags expectedFlag) {
    long traceId = ThreadLocalRandom.current().nextLong();
    long spanId = ThreadLocalRandom.current().nextLong();
    LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    ByteBuf byteBuf = TracingMetadataCodec.encode64(allocator, traceId, spanId, expectedFlag);

    TracingMetadata tracingMetadata = TracingMetadataCodec.decode(byteBuf);

    Assertions.assertThat(tracingMetadata)
        .matches(metadata -> !metadata.isEmpty())
        .matches(tm -> tm.traceIdHigh() == 0)
        .matches(tm -> tm.traceId() == traceId)
        .matches(tm -> tm.spanId() == spanId)
        .matches(tm -> !tm.hasParent())
        .matches(tm -> tm.parentId() == 0)
        .matches(
            tm -> {
              switch (expectedFlag) {
                case UNDECIDED:
                  return !tm.isDecided();
                case NOT_SAMPLE:
                  return tm.isDecided() && !tm.isSampled();
                case SAMPLE:
                  return tm.isDecided() && tm.isSampled();
                case DEBUG:
                  return tm.isDecided() && tm.isDebug();
              }
              return false;
            });
    Assertions.assertThat(byteBuf).matches(ReferenceCounted::release);
    allocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("flags")
  public void shouldEncodeTrace128WithParent(TracingMetadataCodec.Flags expectedFlag) {
    long traceIdHighLocal;
    do {
      traceIdHighLocal = ThreadLocalRandom.current().nextLong();

    } while (traceIdHighLocal == 0);
    long traceIdHigh = traceIdHighLocal;
    long traceId = ThreadLocalRandom.current().nextLong();
    long spanId = ThreadLocalRandom.current().nextLong();
    long parentId = ThreadLocalRandom.current().nextLong();
    LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    ByteBuf byteBuf =
        TracingMetadataCodec.encode128(
            allocator, traceIdHigh, traceId, spanId, parentId, expectedFlag);

    TracingMetadata tracingMetadata = TracingMetadataCodec.decode(byteBuf);

    Assertions.assertThat(tracingMetadata)
        .matches(metadata -> !metadata.isEmpty())
        .matches(tm -> tm.traceIdHigh() == traceIdHigh)
        .matches(tm -> tm.traceId() == traceId)
        .matches(tm -> tm.spanId() == spanId)
        .matches(tm -> tm.hasParent())
        .matches(tm -> tm.parentId() == parentId)
        .matches(
            tm -> {
              switch (expectedFlag) {
                case UNDECIDED:
                  return !tm.isDecided();
                case NOT_SAMPLE:
                  return tm.isDecided() && !tm.isSampled();
                case SAMPLE:
                  return tm.isDecided() && tm.isSampled();
                case DEBUG:
                  return tm.isDecided() && tm.isDebug();
              }
              return false;
            });
    Assertions.assertThat(byteBuf).matches(ReferenceCounted::release);
    allocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("flags")
  public void shouldEncodeTrace128(TracingMetadataCodec.Flags expectedFlag) {
    long traceIdHighLocal;
    do {
      traceIdHighLocal = ThreadLocalRandom.current().nextLong();

    } while (traceIdHighLocal == 0);
    long traceIdHigh = traceIdHighLocal;
    long traceId = ThreadLocalRandom.current().nextLong();
    long spanId = ThreadLocalRandom.current().nextLong();
    LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    ByteBuf byteBuf =
        TracingMetadataCodec.encode128(allocator, traceIdHigh, traceId, spanId, expectedFlag);

    TracingMetadata tracingMetadata = TracingMetadataCodec.decode(byteBuf);

    Assertions.assertThat(tracingMetadata)
        .matches(metadata -> !metadata.isEmpty())
        .matches(tm -> tm.traceIdHigh() == traceIdHigh)
        .matches(tm -> tm.traceId() == traceId)
        .matches(tm -> tm.spanId() == spanId)
        .matches(tm -> !tm.hasParent())
        .matches(tm -> tm.parentId() == 0)
        .matches(
            tm -> {
              switch (expectedFlag) {
                case UNDECIDED:
                  return !tm.isDecided();
                case NOT_SAMPLE:
                  return tm.isDecided() && !tm.isSampled();
                case SAMPLE:
                  return tm.isDecided() && tm.isSampled();
                case DEBUG:
                  return tm.isDecided() && tm.isDebug();
              }
              return false;
            });
    Assertions.assertThat(byteBuf).matches(ReferenceCounted::release);
    allocator.assertHasNoLeaks();
  }
}
