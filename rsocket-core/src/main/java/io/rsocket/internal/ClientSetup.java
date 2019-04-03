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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.keepalive.KeepAliveConnection;
import io.rsocket.resume.ClientRSocketSession;
import io.rsocket.resume.ResumableFramesStore;
import io.rsocket.resume.ResumeStrategy;
import java.time.Duration;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

public interface ClientSetup {
  /*Provide different connections for SETUP / RESUME cases*/
  DuplexConnection wrappedConnection(KeepAliveConnection duplexConnection);

  /*Provide different resume tokens for SETUP / RESUME cases*/
  ByteBuf resumeToken();

  class DefaultClientSetup implements ClientSetup {

    @Override
    public DuplexConnection wrappedConnection(KeepAliveConnection connection) {
      return connection;
    }

    @Override
    public ByteBuf resumeToken() {
      return Unpooled.EMPTY_BUFFER;
    }
  }

  class ResumableClientSetup implements ClientSetup {
    private final ByteBuf resumeToken;
    private final ByteBufAllocator allocator;
    private final Mono<KeepAliveConnection> newConnectionFactory;
    private final Duration resumeSessionDuration;
    private final Supplier<ResumeStrategy> resumeStrategySupplier;
    private final ResumableFramesStore resumableFramesStore;
    private final Duration resumeStreamTimeout;

    public ResumableClientSetup(
        ByteBufAllocator allocator,
        Mono<KeepAliveConnection> newConnectionFactory,
        ByteBuf resumeToken,
        ResumableFramesStore resumableFramesStore,
        Duration resumeSessionDuration,
        Duration resumeStreamTimeout,
        Supplier<ResumeStrategy> resumeStrategySupplier) {
      this.allocator = allocator;
      this.newConnectionFactory = newConnectionFactory;
      this.resumeToken = resumeToken;
      this.resumeSessionDuration = resumeSessionDuration;
      this.resumeStrategySupplier = resumeStrategySupplier;
      this.resumableFramesStore = resumableFramesStore;
      this.resumeStreamTimeout = resumeStreamTimeout;
    }

    @Override
    public DuplexConnection wrappedConnection(KeepAliveConnection connection) {
      ClientRSocketSession rSocketSession =
          new ClientRSocketSession(
                  connection,
                  allocator,
                  resumeSessionDuration,
                  resumeStrategySupplier,
                  resumableFramesStore,
                  resumeStreamTimeout)
              .continueWith(newConnectionFactory)
              .resumeToken(resumeToken);

      return rSocketSession.resumableConnection();
    }

    @Override
    public ByteBuf resumeToken() {
      return resumeToken;
    }
  }
}
