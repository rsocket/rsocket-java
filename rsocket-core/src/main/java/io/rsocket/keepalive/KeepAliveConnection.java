/*
 * Copyright 2015-2019 the original author or authors.
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

package io.rsocket.keepalive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.internal.KeepAliveData;
import io.rsocket.resume.ResumePositionsConnection;
import io.rsocket.resume.ResumeStateHolder;
import io.rsocket.util.DuplexConnectionProxy;
import io.rsocket.util.Function3;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.*;

public class KeepAliveConnection extends DuplexConnectionProxy
    implements ResumePositionsConnection {

  private final MonoProcessor<KeepAliveHandler> keepAliveHandlerReady = MonoProcessor.create();
  private final ByteBufAllocator allocator;
  private final Function<ByteBuf, Mono<KeepAliveData>> keepAliveData;
  private final Function3<ByteBufAllocator, Duration, Duration, KeepAliveHandler>
      keepAliveHandlerFactory;
  private final Consumer<Throwable> errorConsumer;
  private volatile KeepAliveHandler keepAliveHandler;
  private volatile ResumeStateHolder resumeStateHolder;

  public static KeepAliveConnection ofClient(
      ByteBufAllocator allocator,
      DuplexConnection duplexConnection,
      Function<ByteBuf, Mono<KeepAliveData>> keepAliveData,
      Consumer<Throwable> errorConsumer) {

    return new KeepAliveConnection(
        allocator, duplexConnection, keepAliveData, KeepAliveHandler::ofClient, errorConsumer);
  }

  public static KeepAliveConnection ofServer(
      ByteBufAllocator allocator,
      DuplexConnection duplexConnection,
      Function<ByteBuf, Mono<KeepAliveData>> keepAliveData,
      Consumer<Throwable> errorConsumer) {

    return new KeepAliveConnection(
        allocator, duplexConnection, keepAliveData, KeepAliveHandler::ofServer, errorConsumer);
  }

  private KeepAliveConnection(
      ByteBufAllocator allocator,
      DuplexConnection duplexConnection,
      Function<ByteBuf, Mono<KeepAliveData>> keepAliveData,
      Function3<ByteBufAllocator, Duration, Duration, KeepAliveHandler> keepAliveHandlerFactory,
      Consumer<Throwable> errorConsumer) {
    super(duplexConnection);
    this.allocator = allocator;
    this.keepAliveData = keepAliveData;
    this.keepAliveHandlerFactory = keepAliveHandlerFactory;
    this.errorConsumer = errorConsumer;
    keepAliveHandlerReady.subscribe(this::startKeepAlives);
  }

  private void startKeepAlives(KeepAliveHandler keepAliveHandler) {
    this.keepAliveHandler = keepAliveHandler;

    send(keepAliveHandler.send()).subscribe(null, err -> keepAliveHandler.dispose());

    keepAliveHandler
        .timeout()
        .subscribe(
            keepAlive -> {
              String message =
                  String.format("No keep-alive acks for %d ms", keepAlive.getTimeoutMillis());
              ConnectionErrorException err = new ConnectionErrorException(message);
              errorConsumer.accept(err);
              dispose();
            });
    keepAliveHandler.start();
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return super.send(
        Flux.from(frames)
            .doOnNext(
                f -> {
                  if (isStartFrame(f)) {
                    keepAliveHandler(keepAliveData.apply(f)).subscribe(keepAliveHandlerReady);
                  }
                }));
  }

  @Override
  public Flux<ByteBuf> receive() {
    return super.receive()
        .doOnNext(
            f -> {
              if (isKeepAliveFrame(f)) {
                long receivedPos = keepAliveHandler.receive(f);
                if (isResumeRequested() && receivedPos > 0) {
                  resumeStateHolder.onImpliedPosition(receivedPos);
                }
              } else if (isStartFrame(f)) {
                keepAliveHandler(keepAliveData.apply(f)).subscribe(keepAliveHandlerReady);
              }
            });
  }

  @Override
  public void dispose() {
    KeepAliveHandler keepAliveHandler = keepAliveHandlerReady.peek();
    if (keepAliveHandler != null) {
      keepAliveHandler.dispose();
    }
    super.dispose();
  }

  @Override
  public void acceptResumeState(ResumeStateHolder resumeStateHolder) {
    this.resumeStateHolder = resumeStateHolder;
    keepAliveHandlerReady.subscribe(h -> h.resumeState(resumeStateHolder));
  }

  private boolean isResumeRequested() {
    return keepAliveHandler.hasResumeState();
  }

  private static boolean isStartFrame(ByteBuf frame) {
    return FrameHeaderFlyweight.frameType(frame) == FrameType.SETUP
        || FrameHeaderFlyweight.frameType(frame) == FrameType.RESUME;
  }

  private static boolean isKeepAliveFrame(ByteBuf frame) {
    return FrameHeaderFlyweight.frameType(frame) == FrameType.KEEPALIVE;
  }

  private Mono<KeepAliveHandler> keepAliveHandler(Mono<KeepAliveData> keepAliveData) {
    return keepAliveData.map(
        kad -> keepAliveHandlerFactory.apply(allocator, kad.getTickPeriod(), kad.getTimeout()));
  }
}
