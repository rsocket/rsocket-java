package io.rsocket.plugins;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameType;
import io.rsocket.internal.jctools.queues.MpscUnboundedArrayQueue;
import java.util.Queue;
import org.assertj.core.api.Assertions;
import reactor.core.publisher.SignalType;

public class TestRequestInterceptor implements RequestInterceptor {

  final Queue<Event> events = new MpscUnboundedArrayQueue<>(128);

  @Override
  public void dispose() {}

  @Override
  public void onStart(int streamId, FrameType requestType, ByteBuf metadata) {
    events.add(new Event(EventType.ON_START, streamId, requestType, null));
  }

  @Override
  public void onEnd(int streamId, SignalType terminalSignal) {
    events.add(new Event(EventType.ON_END, streamId, null, terminalSignal));
  }

  @Override
  public void onReject(int streamId, FrameType requestType, ByteBuf metadata) {
    events.add(new Event(EventType.ON_REJECT, streamId, requestType, null));
  }

  public TestRequestInterceptor expectOnStart(int streamId, FrameType requestType) {
    final Event event = events.poll();

    Assertions.assertThat(event)
        .hasFieldOrPropertyWithValue("eventType", EventType.ON_START)
        .hasFieldOrPropertyWithValue("streamId", streamId)
        .hasFieldOrPropertyWithValue("requestType", requestType);

    return this;
  }

  public TestRequestInterceptor expectOnEnd(int streamId, SignalType signalType) {
    final Event event = events.poll();

    Assertions.assertThat(event)
        .hasFieldOrPropertyWithValue("eventType", EventType.ON_END)
        .hasFieldOrPropertyWithValue("streamId", streamId)
        .hasFieldOrPropertyWithValue("signalType", signalType);

    return this;
  }

  public TestRequestInterceptor expectNothing() {
    final Event event = events.poll();

    Assertions.assertThat(event).isNull();

    return this;
  }

  static final class Event {
    final EventType eventType;
    final int streamId;
    final FrameType requestType;
    final SignalType signalType;

    Event(EventType eventType, int streamId, FrameType requestType, SignalType signalType) {
      this.eventType = eventType;
      this.streamId = streamId;
      this.requestType = requestType;
      this.signalType = signalType;
    }
  }

  enum EventType {
    ON_START,
    ON_END,
    ON_REJECT
  }
}
