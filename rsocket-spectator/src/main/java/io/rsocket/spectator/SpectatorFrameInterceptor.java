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

package io.rsocket.spectator;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.framing.FrameType;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** An implementation of {@link DuplexConnectionInterceptor} that uses Spectator */
public class SpectatorFrameInterceptor implements DuplexConnectionInterceptor {
  private final Registry registry;

  public SpectatorFrameInterceptor(Registry registry) {
    this.registry = registry;
  }

  @Override
  public DuplexConnection apply(Type type, DuplexConnection connection) {
    return new DuplexConnection() {
      Counter cancelCounter = registry.counter(FrameType.CANCEL.name(), type.name());
      Counter requestChannelCounter =
          registry.counter(FrameType.REQUEST_CHANNEL.name(), type.name());
      Counter completeCounter = registry.counter(FrameType.COMPLETE.name(), type.name());
      Counter errorCounter = registry.counter(FrameType.ERROR.name(), type.name());
      Counter extCounter = registry.counter(FrameType.EXT.name(), type.name());
      Counter fireAndForgetCounter =
          registry.counter(FrameType.REQUEST_FNF.name(), type.name());
      Counter keepAliveCounter = registry.counter(FrameType.KEEPALIVE.name(), type.name());
      Counter leaseCounter = registry.counter(FrameType.LEASE.name(), type.name());
      Counter metadataPushCounter = registry.counter(FrameType.METADATA_PUSH.name(), type.name());
      Counter nextCounter = registry.counter(FrameType.NEXT.name(), type.name());
      Counter nextCompleteCounter = registry.counter(FrameType.NEXT_COMPLETE.name(), type.name());
      Counter payloadCounter = registry.counter(FrameType.PAYLOAD.name(), type.name());
      Counter requestNCounter = registry.counter(FrameType.REQUEST_N.name(), type.name());
      Counter requestResponseCounter =
          registry.counter(FrameType.REQUEST_RESPONSE.name(), type.name());
      Counter requestStreamCounter = registry.counter(FrameType.REQUEST_STREAM.name(), type.name());
      Counter resumeCounter = registry.counter(FrameType.RESUME.name(), type.name());
      Counter resumeOkCounter = registry.counter(FrameType.RESUME_OK.name(), type.name());
      Counter setupCounter = registry.counter(FrameType.SETUP.name(), type.name());
      Counter undefinedCounter = registry.counter(FrameType.RESERVED.name(), type.name());

      @Override
      public Mono<Void> send(Publisher<Frame> frame) {
        return connection.send(Flux.from(frame).doOnNext(this::count));
      }

      @Override
      public Mono<Void> sendOne(Frame frame) {
        return Mono.defer(
            () -> {
              count(frame);
              return connection.sendOne(frame);
            });
      }

      @Override
      public Flux<Frame> receive() {
        return connection.receive().doOnNext(this::count);
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

      @Override
      public double availability() {
        return connection.availability();
      }

      private void count(Frame frame) {
        switch (frame.getType()) {
          case CANCEL:
            cancelCounter.increment();
            break;
          case REQUEST_CHANNEL:
            requestChannelCounter.increment();
            break;
          case COMPLETE:
            completeCounter.increment();
            break;
          case ERROR:
            errorCounter.increment();
            break;
          case EXT:
            extCounter.increment();
            break;
          case REQUEST_FNF:
            fireAndForgetCounter.increment();
            break;
          case KEEPALIVE:
            keepAliveCounter.increment();
            break;
          case LEASE:
            leaseCounter.increment();
            break;
          case METADATA_PUSH:
            metadataPushCounter.increment();
            break;
          case NEXT:
            nextCounter.increment();
            break;
          case NEXT_COMPLETE:
            nextCompleteCounter.increment();
            break;
          case PAYLOAD:
            payloadCounter.increment();
            break;
          case REQUEST_N:
            requestNCounter.increment();
            break;
          case REQUEST_RESPONSE:
            requestResponseCounter.increment();
            break;
          case REQUEST_STREAM:
            requestStreamCounter.increment();
            break;
          case RESUME:
            resumeCounter.increment();
            break;
          case RESUME_OK:
            resumeOkCounter.increment();
            break;
          case SETUP:
            setupCounter.increment();
            break;
          case RESERVED:
          default:
            undefinedCounter.increment();
            break;
        }
      }
    };
  }
}
