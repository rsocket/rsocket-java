/*
 * Copyright 2015-2020 the original author or authors.
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

package io.rsocket.micrometer;

import static io.rsocket.frame.FrameType.*;

import io.micrometer.core.instrument.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.plugins.DuplexConnectionInterceptor.Type;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * An implementation of {@link DuplexConnection} that intercepts frames and gathers Micrometer
 * metrics about them.
 *
 * <p>The metric is called {@code rsocket.frame} and is tagged with {@code connection.type} ({@link
 * Type}), {@code frame.type} ({@link FrameType}), and any additional configured tags. {@code
 * rsocket.duplex.connection.close} and {@code rsocket.duplex.connection.dispose} metrics, tagged
 * with {@code connection.type} ({@link Type}) and any additional configured tags are also
 * collected.
 *
 * @see <a href="https://micrometer.io">Micrometer</a>
 */
final class MicrometerDuplexConnection implements DuplexConnection {

  private final Counter close;

  private final DuplexConnection delegate;

  private final Counter dispose;

  private final FrameCounters frameCounters;

  /**
   * Creates a new {@link DuplexConnection}.
   *
   * @param connectionType the type of connection being monitored
   * @param delegate the {@link DuplexConnection} to delegate to
   * @param meterRegistry the {@link MeterRegistry} to use
   * @param tags additional tags to attach to {@link Meter}s
   * @throws NullPointerException if {@code connectionType}, {@code delegate}, or {@code
   *     meterRegistry} is {@code null}
   */
  MicrometerDuplexConnection(
      Type connectionType, DuplexConnection delegate, MeterRegistry meterRegistry, Tag... tags) {

    Objects.requireNonNull(connectionType, "connectionType must not be null");
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");

    this.close =
        meterRegistry.counter(
            "rsocket.duplex.connection.close",
            Tags.of(tags).and("connection.type", connectionType.name()));
    this.dispose =
        meterRegistry.counter(
            "rsocket.duplex.connection.dispose",
            Tags.of(tags).and("connection.type", connectionType.name()));
    this.frameCounters = new FrameCounters(connectionType, meterRegistry, tags);
  }

  @Override
  public ByteBufAllocator alloc() {
    return delegate.alloc();
  }

  @Override
  public SocketAddress localAddress() {
    return delegate.localAddress();
  }

  @Override
  public SocketAddress remoteAddress() {
    return delegate.remoteAddress();
  }

  @Override
  public void dispose() {
    delegate.dispose();
    dispose.increment();
  }

  @Override
  public Mono<Void> onClose() {
    return delegate.onClose().doAfterTerminate(close::increment);
  }

  @Override
  public Flux<ByteBuf> receive() {
    return delegate.receive().doOnNext(frameCounters);
  }

  @Override
  public void sendFrame(int streamId, ByteBuf frame) {
    frameCounters.accept(frame);
    delegate.sendFrame(streamId, frame);
  }

  @Override
  public void sendErrorAndClose(RSocketErrorException e) {
    delegate.sendErrorAndClose(e);
  }

  private static final class FrameCounters implements Consumer<ByteBuf> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Counter cancel;

    private final Counter complete;

    private final Counter error;

    private final Counter extension;

    private final Counter keepalive;

    private final Counter lease;

    private final Counter metadataPush;

    private final Counter next;

    private final Counter nextComplete;

    private final Counter payload;

    private final Counter requestChannel;

    private final Counter requestFireAndForget;

    private final Counter requestN;

    private final Counter requestResponse;

    private final Counter requestStream;

    private final Counter resume;

    private final Counter resumeOk;

    private final Counter setup;

    private final Counter unknown;

    private FrameCounters(Type connectionType, MeterRegistry meterRegistry, Tag... tags) {
      this.cancel = counter(connectionType, meterRegistry, CANCEL, tags);
      this.complete = counter(connectionType, meterRegistry, COMPLETE, tags);
      this.error = counter(connectionType, meterRegistry, ERROR, tags);
      this.extension = counter(connectionType, meterRegistry, EXT, tags);
      this.keepalive = counter(connectionType, meterRegistry, KEEPALIVE, tags);
      this.lease = counter(connectionType, meterRegistry, LEASE, tags);
      this.metadataPush = counter(connectionType, meterRegistry, METADATA_PUSH, tags);
      this.next = counter(connectionType, meterRegistry, NEXT, tags);
      this.nextComplete = counter(connectionType, meterRegistry, NEXT_COMPLETE, tags);
      this.payload = counter(connectionType, meterRegistry, PAYLOAD, tags);
      this.requestChannel = counter(connectionType, meterRegistry, REQUEST_CHANNEL, tags);
      this.requestFireAndForget = counter(connectionType, meterRegistry, REQUEST_FNF, tags);
      this.requestN = counter(connectionType, meterRegistry, REQUEST_N, tags);
      this.requestResponse = counter(connectionType, meterRegistry, REQUEST_RESPONSE, tags);
      this.requestStream = counter(connectionType, meterRegistry, REQUEST_STREAM, tags);
      this.resume = counter(connectionType, meterRegistry, RESUME, tags);
      this.resumeOk = counter(connectionType, meterRegistry, RESUME_OK, tags);
      this.setup = counter(connectionType, meterRegistry, SETUP, tags);
      this.unknown = counter(connectionType, meterRegistry, "UNKNOWN", tags);
    }

    private static Counter counter(
        Type connectionType, MeterRegistry meterRegistry, FrameType frameType, Tag... tags) {

      return counter(connectionType, meterRegistry, frameType.name(), tags);
    }

    private static Counter counter(
        Type connectionType, MeterRegistry meterRegistry, String frameType, Tag... tags) {

      return meterRegistry.counter(
          "rsocket.frame",
          Tags.of(tags).and("connection.type", connectionType.name()).and("frame.type", frameType));
    }

    @Override
    public void accept(ByteBuf frame) {
      FrameType frameType = FrameHeaderCodec.frameType(frame);

      switch (frameType) {
        case SETUP:
          this.setup.increment();
          break;
        case LEASE:
          this.lease.increment();
          break;
        case KEEPALIVE:
          this.keepalive.increment();
          break;
        case REQUEST_RESPONSE:
          this.requestResponse.increment();
          break;
        case REQUEST_FNF:
          this.requestFireAndForget.increment();
          break;
        case REQUEST_STREAM:
          this.requestStream.increment();
          break;
        case REQUEST_CHANNEL:
          this.requestChannel.increment();
          break;
        case REQUEST_N:
          this.requestN.increment();
          break;
        case CANCEL:
          this.cancel.increment();
          break;
        case PAYLOAD:
          this.payload.increment();
          break;
        case ERROR:
          this.error.increment();
          break;
        case METADATA_PUSH:
          this.metadataPush.increment();
          break;
        case RESUME:
          this.resume.increment();
          break;
        case RESUME_OK:
          this.resumeOk.increment();
          break;
        case NEXT:
          this.next.increment();
          break;
        case COMPLETE:
          this.complete.increment();
          break;
        case NEXT_COMPLETE:
          this.nextComplete.increment();
          break;
        case EXT:
          this.extension.increment();
          break;
        default:
          this.logger.debug("Skipping count of unknown frame type: {}", frameType);
          this.unknown.increment();
      }
    }
  }
}
