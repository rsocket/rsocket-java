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

package io.rsocket.internal;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.FrameUtil;
import io.rsocket.plugins.DuplexConnectionInterceptor.Type;
import io.rsocket.plugins.PluginRegistry;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * {@link DuplexConnection#receive()} is a single stream on which the following type of frames
 * arrive:
 *
 * <ul>
 *   <li>Frames for streams initiated by the initiator of the connection (client).
 *   <li>Frames for streams initiated by the acceptor of the connection (server).
 * </ul>
 *
 * <p>The only way to differentiate these two frames is determining whether the stream Id is odd or
 * even. Even IDs are for the streams initiated by server and odds are for streams initiated by the
 * client.
 */
public class ClientServerInputMultiplexer implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger("io.rsocket.FrameLogger");

  private final DuplexConnection streamZeroConnection;
  private final DuplexConnection serverConnection;
  private final DuplexConnection clientConnection;
  private final DuplexConnection source;

  public ClientServerInputMultiplexer(DuplexConnection source, PluginRegistry plugins) {
    this.source = source;
    final MonoProcessor<Flux<ByteBuf>> streamZero = MonoProcessor.create();
    final MonoProcessor<Flux<ByteBuf>> server = MonoProcessor.create();
    final MonoProcessor<Flux<ByteBuf>> client = MonoProcessor.create();

    source = plugins.applyConnection(Type.SOURCE, source);
    streamZeroConnection =
        plugins.applyConnection(Type.STREAM_ZERO, new InternalDuplexConnection(source, streamZero));
    serverConnection =
        plugins.applyConnection(Type.SERVER, new InternalDuplexConnection(source, server));
    clientConnection =
        plugins.applyConnection(Type.CLIENT, new InternalDuplexConnection(source, client));

    source
        .receive()
        .groupBy(
            frame -> {
              int streamId = FrameHeaderFlyweight.streamId(frame);
              final Type type;
              if (streamId == 0) {
                if (FrameHeaderFlyweight.frameType(frame) == FrameType.SETUP) {
                  type = Type.STREAM_ZERO;
                } else {
                  type = Type.CLIENT;
                }
              } else if ((streamId & 0b1) == 0) {
                type = Type.SERVER;
              } else {
                type = Type.CLIENT;
              }
              return type;
            })
        .subscribe(
            group -> {
              switch (group.key()) {
                case STREAM_ZERO:
                  streamZero.onNext(group);
                  break;

                case SERVER:
                  server.onNext(group);
                  break;

                case CLIENT:
                  client.onNext(group);
                  break;
              }
            },
            t -> {
              LOGGER.error("Error receiving frame:", t);
              dispose();
            });
  }

  public DuplexConnection asServerConnection() {
    return serverConnection;
  }

  public DuplexConnection asClientConnection() {
    return clientConnection;
  }

  public DuplexConnection asStreamZeroConnection() {
    return streamZeroConnection;
  }

  @Override
  public void dispose() {
    source.dispose();
  }

  @Override
  public boolean isDisposed() {
    return source.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return source.onClose();
  }

  private static class InternalDuplexConnection implements DuplexConnection {
    private final DuplexConnection source;
    private final MonoProcessor<Flux<ByteBuf>> processor;
    private final boolean debugEnabled;

    public InternalDuplexConnection(
        DuplexConnection source, MonoProcessor<Flux<ByteBuf>> processor) {
      this.source = source;
      this.processor = processor;
      this.debugEnabled = LOGGER.isDebugEnabled();
    }

    @Override
    public Mono<Void> send(Publisher<ByteBuf> frame) {
      if (debugEnabled) {
        frame = Flux.from(frame).doOnNext(f -> LOGGER.debug("sending -> " + FrameUtil.toString(f)));
      }

      return source.send(frame);
    }

    @Override
    public Mono<Void> sendOne(ByteBuf frame) {
      if (debugEnabled) {
        LOGGER.debug("sending -> " + FrameUtil.toString(frame));
      }

      return source.sendOne(frame);
    }

    @Override
    public Flux<ByteBuf> receive() {
      return processor.flatMapMany(
          f -> {
            if (debugEnabled) {
              return f.doOnNext(frame -> LOGGER.debug("receiving -> " + FrameUtil.toString(frame)));
            } else {
              return f;
            }
          });
    }

    @Override
    public void dispose() {
      source.dispose();
    }

    @Override
    public boolean isDisposed() {
      return source.isDisposed();
    }

    @Override
    public Mono<Void> onClose() {
      return source.onClose();
    }

    @Override
    public double availability() {
      return source.availability();
    }
  }
}
