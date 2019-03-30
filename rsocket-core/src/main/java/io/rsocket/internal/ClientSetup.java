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
import io.rsocket.resume.*;
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
    private final ClientResumeConfiguration config;
    private final ByteBufAllocator allocator;
    private final Mono<KeepAliveConnection> newConnection;

    public ResumableClientSetup(
        ByteBufAllocator allocator,
        Mono<KeepAliveConnection> newConnection,
        ByteBuf resumeToken,
        ResumableFramesStore resumableFramesStore,
        Duration resumeSessionDuration,
        Duration resumeStreamTimeout,
        Supplier<ResumeStrategy> resumeStrategySupplier) {
      this.allocator = allocator;
      this.newConnection = newConnection;
      this.resumeToken = resumeToken;
      this.config =
          new ClientResumeConfiguration(
              resumeSessionDuration,
              resumeStrategySupplier,
              resumableFramesStore,
              resumeStreamTimeout);
    }

    @Override
    public DuplexConnection wrappedConnection(KeepAliveConnection connection) {
      ClientRSocketSession rSocketSession =
          new ClientRSocketSession(allocator, connection, config)
              .continueWith(newConnection)
              .resumeToken(resumeToken);

      return rSocketSession.resumableConnection();
    }

    @Override
    public ByteBuf resumeToken() {
      return resumeToken;
    }
  }
}
