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

package io.rsocket.core;

import static io.rsocket.keepalive.KeepAliveHandler.*;

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.exceptions.RejectedResumeException;
import io.rsocket.exceptions.UnsupportedSetupException;
import io.rsocket.frame.ResumeFrameCodec;
import io.rsocket.frame.SetupFrameCodec;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.resume.*;
import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

abstract class ServerSetup {

  Mono<Tuple2<ByteBuf, DuplexConnection>> init(DuplexConnection connection) {
    return Mono.create(
        sink -> sink.onRequest(__ -> new SetupHandlingDuplexConnection(connection, sink)));
  }

  abstract Mono<Void> acceptRSocketSetup(
      ByteBuf frame,
      DuplexConnection clientServerConnection,
      BiFunction<KeepAliveHandler, DuplexConnection, Mono<Void>> then);

  abstract Mono<Void> acceptRSocketResume(ByteBuf frame, DuplexConnection connection);

  void dispose() {}

  void sendError(DuplexConnection duplexConnection, RSocketErrorException exception) {
    duplexConnection.sendErrorAndClose(exception);
  }

  static class DefaultServerSetup extends ServerSetup {

    @Override
    public Mono<Void> acceptRSocketSetup(
        ByteBuf frame,
        DuplexConnection duplexConnection,
        BiFunction<KeepAliveHandler, DuplexConnection, Mono<Void>> then) {

      if (SetupFrameCodec.resumeEnabled(frame)) {
        sendError(duplexConnection, new UnsupportedSetupException("resume not supported"));
        return Mono.empty();
      } else {
        return then.apply(new DefaultKeepAliveHandler(duplexConnection), duplexConnection);
      }
    }

    @Override
    public Mono<Void> acceptRSocketResume(ByteBuf frame, DuplexConnection duplexConnection) {
      sendError(duplexConnection, new RejectedResumeException("resume not supported"));
      return duplexConnection.onClose();
    }
  }

  static class ResumableServerSetup extends ServerSetup {
    private final SessionManager sessionManager;
    private final Duration resumeSessionDuration;
    private final Duration resumeStreamTimeout;
    private final Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory;
    private final boolean cleanupStoreOnKeepAlive;

    ResumableServerSetup(
        SessionManager sessionManager,
        Duration resumeSessionDuration,
        Duration resumeStreamTimeout,
        Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory,
        boolean cleanupStoreOnKeepAlive) {
      this.sessionManager = sessionManager;
      this.resumeSessionDuration = resumeSessionDuration;
      this.resumeStreamTimeout = resumeStreamTimeout;
      this.resumeStoreFactory = resumeStoreFactory;
      this.cleanupStoreOnKeepAlive = cleanupStoreOnKeepAlive;
    }

    @Override
    public Mono<Void> acceptRSocketSetup(
        ByteBuf frame,
        DuplexConnection duplexConnection,
        BiFunction<KeepAliveHandler, DuplexConnection, Mono<Void>> then) {

      if (SetupFrameCodec.resumeEnabled(frame)) {
        ByteBuf resumeToken = SetupFrameCodec.resumeToken(frame);

        final ResumableFramesStore resumableFramesStore = resumeStoreFactory.apply(resumeToken);
        final ResumableDuplexConnection resumableDuplexConnection =
            new ResumableDuplexConnection(duplexConnection, resumableFramesStore);
        final ServerRSocketSession serverRSocketSession =
            new ServerRSocketSession(
                duplexConnection,
                resumableDuplexConnection,
                resumeSessionDuration,
                resumableFramesStore,
                resumeToken,
                cleanupStoreOnKeepAlive);

        sessionManager.save(serverRSocketSession);

        return then.apply(
            new ResumableKeepAliveHandler(
                resumableDuplexConnection, serverRSocketSession, serverRSocketSession),
            resumableDuplexConnection);
      } else {
        return then.apply(new DefaultKeepAliveHandler(duplexConnection), duplexConnection);
      }
    }

    @Override
    public Mono<Void> acceptRSocketResume(ByteBuf frame, DuplexConnection duplexConnection) {
      ServerRSocketSession session = sessionManager.get(ResumeFrameCodec.token(frame));
      if (session != null) {
        return session.resumeWith(frame, duplexConnection);
      } else {
        sendError(duplexConnection, new RejectedResumeException("unknown resume token"));
        return duplexConnection.onClose();
      }
    }

    @Override
    public void dispose() {
      sessionManager.dispose();
    }
  }
}
