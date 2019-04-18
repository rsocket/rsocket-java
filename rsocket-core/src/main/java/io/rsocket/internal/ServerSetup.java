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

package io.rsocket.internal;

import static io.rsocket.keepalive.KeepAliveHandler.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.exceptions.RejectedResumeException;
import io.rsocket.exceptions.UnsupportedSetupException;
import io.rsocket.frame.ResumeFrameFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.resume.*;
import io.rsocket.util.ConnectionUtils;
import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import reactor.core.publisher.Mono;

public interface ServerSetup {

  Mono<Void> acceptRSocketSetup(
      ByteBuf frame,
      ClientServerInputMultiplexer multiplexer,
      BiFunction<KeepAliveHandler, ClientServerInputMultiplexer, Mono<Void>> then);

  Mono<Void> acceptRSocketResume(ByteBuf frame, ClientServerInputMultiplexer multiplexer);

  default void dispose() {}

  class DefaultServerSetup implements ServerSetup {
    private final ByteBufAllocator allocator;

    public DefaultServerSetup(ByteBufAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public Mono<Void> acceptRSocketSetup(
        ByteBuf frame,
        ClientServerInputMultiplexer multiplexer,
        BiFunction<KeepAliveHandler, ClientServerInputMultiplexer, Mono<Void>> then) {

      if (SetupFrameFlyweight.resumeEnabled(frame)) {
        return sendError(multiplexer, new UnsupportedSetupException("resume not supported"))
            .doFinally(
                signalType -> {
                  frame.release();
                  multiplexer.dispose();
                });
      } else {
        return then.apply(new DefaultKeepAliveHandler(), multiplexer);
      }
    }

    @Override
    public Mono<Void> acceptRSocketResume(ByteBuf frame, ClientServerInputMultiplexer multiplexer) {

      return sendError(multiplexer, new RejectedResumeException("resume not supported"))
          .doFinally(
              signalType -> {
                frame.release();
                multiplexer.dispose();
              });
    }

    private Mono<Void> sendError(ClientServerInputMultiplexer multiplexer, Exception exception) {
      return ConnectionUtils.sendError(allocator, multiplexer, exception);
    }
  }

  class ResumableServerSetup implements ServerSetup {
    private final ByteBufAllocator allocator;
    private final SessionManager sessionManager;
    private final Duration resumeSessionDuration;
    private final Duration resumeStreamTimeout;
    private final Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory;
    private final boolean cleanupStoreOnKeepAlive;

    public ResumableServerSetup(
        ByteBufAllocator allocator,
        SessionManager sessionManager,
        Duration resumeSessionDuration,
        Duration resumeStreamTimeout,
        Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory,
        boolean cleanupStoreOnKeepAlive) {
      this.allocator = allocator;
      this.sessionManager = sessionManager;
      this.resumeSessionDuration = resumeSessionDuration;
      this.resumeStreamTimeout = resumeStreamTimeout;
      this.resumeStoreFactory = resumeStoreFactory;
      this.cleanupStoreOnKeepAlive = cleanupStoreOnKeepAlive;
    }

    @Override
    public Mono<Void> acceptRSocketSetup(
        ByteBuf frame,
        ClientServerInputMultiplexer multiplexer,
        BiFunction<KeepAliveHandler, ClientServerInputMultiplexer, Mono<Void>> then) {

      if (SetupFrameFlyweight.resumeEnabled(frame)) {
        ByteBuf resumeToken = SetupFrameFlyweight.resumeToken(frame);

        ResumableDuplexConnection connection =
            sessionManager
                .save(
                    new ServerRSocketSession(
                        multiplexer.asClientServerConnection(),
                        allocator,
                        resumeSessionDuration,
                        resumeStreamTimeout,
                        resumeStoreFactory,
                        resumeToken,
                        cleanupStoreOnKeepAlive))
                .resumableConnection();
        return then.apply(
            new ResumableKeepAliveHandler(connection),
            new ClientServerInputMultiplexer(connection));
      } else {
        return then.apply(new DefaultKeepAliveHandler(), multiplexer);
      }
    }

    @Override
    public Mono<Void> acceptRSocketResume(ByteBuf frame, ClientServerInputMultiplexer multiplexer) {
      ServerRSocketSession session = sessionManager.get(ResumeFrameFlyweight.token(frame));
      if (session != null) {
        return session
            .continueWith(multiplexer.asClientServerConnection())
            .resumeWith(frame)
            .onClose()
            .then();
      } else {
        return sendError(multiplexer, new RejectedResumeException("unknown resume token"))
            .doFinally(
                s -> {
                  frame.release();
                  multiplexer.dispose();
                });
      }
    }

    private Mono<Void> sendError(ClientServerInputMultiplexer multiplexer, Exception exception) {
      return ConnectionUtils.sendError(allocator, multiplexer, exception);
    }

    @Override
    public void dispose() {
      sessionManager.dispose();
    }
  }
}
