package io.rsocket.metadata;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Thread)
public class WellKnownMimeTypePerf {

  // this is the old values() looping implementation of fromId
  private WellKnownMimeType fromIdValuesLoop(int id) {
    if (id < 0 || id > 127) {
      return WellKnownMimeType.UNPARSEABLE_MIME_TYPE;
    }
    for (WellKnownMimeType value : WellKnownMimeType.values()) {
      if (value.getIdentifier() == id) {
        return value;
      }
    }
    return WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE;
  }

  // this is the core of the old values() looping implementation of fromMimeType
  private WellKnownMimeType fromStringValuesLoop(String mimeType) {
    for (WellKnownMimeType value : WellKnownMimeType.values()) {
      if (mimeType.equals(value.getMime())) {
        return value;
      }
    }
    return WellKnownMimeType.UNPARSEABLE_MIME_TYPE;
  }

  @Benchmark
  public void fromIdArrayLookup(final Blackhole bh) {
    // negative lookup
    bh.consume(WellKnownMimeType.fromId(-10));
    bh.consume(WellKnownMimeType.fromId(-1));
    // too large lookup
    bh.consume(WellKnownMimeType.fromId(129));
    // first lookup
    bh.consume(WellKnownMimeType.fromId(0));
    // middle lookup
    bh.consume(WellKnownMimeType.fromId(37));
    // reserved lookup
    bh.consume(WellKnownMimeType.fromId(63));
    // last lookup
    bh.consume(WellKnownMimeType.fromId(127));
  }

  @Benchmark
  public void fromIdValuesLoopLookup(final Blackhole bh) {
    // negative lookup
    bh.consume(fromIdValuesLoop(-10));
    bh.consume(fromIdValuesLoop(-1));
    // too large lookup
    bh.consume(fromIdValuesLoop(129));
    // first lookup
    bh.consume(fromIdValuesLoop(0));
    // middle lookup
    bh.consume(fromIdValuesLoop(37));
    // reserved lookup
    bh.consume(fromIdValuesLoop(63));
    // last lookup
    bh.consume(fromIdValuesLoop(127));
  }

  @Benchmark
  public void fromStringMapLookup(final Blackhole bh) {
    // unknown lookup
    bh.consume(WellKnownMimeType.fromMimeType("foo/bar"));
    // first lookup
    bh.consume(WellKnownMimeType.fromMimeType(WellKnownMimeType.APPLICATION_AVRO.getMime()));
    // middle lookup
    bh.consume(WellKnownMimeType.fromMimeType(WellKnownMimeType.VIDEO_VP8.getMime()));
    // last lookup
    bh.consume(
        WellKnownMimeType.fromMimeType(
            WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getMime()));
  }

  @Benchmark
  public void fromStringValuesLoopLookup(final Blackhole bh) {
    // unknown lookup
    bh.consume(fromStringValuesLoop("foo/bar"));
    // first lookup
    bh.consume(fromStringValuesLoop(WellKnownMimeType.APPLICATION_AVRO.getMime()));
    // middle lookup
    bh.consume(fromStringValuesLoop(WellKnownMimeType.VIDEO_VP8.getMime()));
    // last lookup
    bh.consume(
        fromStringValuesLoop(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getMime()));
  }
}
