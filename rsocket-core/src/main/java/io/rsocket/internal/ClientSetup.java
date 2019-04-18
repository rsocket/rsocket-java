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
import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.resume.ClientRSocketSession;
import io.rsocket.resume.ResumableDuplexConnection;
import io.rsocket.resume.ResumableFramesStore;
import io.rsocket.resume.ResumeStrategy;
import java.time.Duration;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

public interface ClientSetup {

  DuplexConnection connection();

  KeepAliveHandler keepAliveHandler();

  ByteBuf resumeToken();

  class DefaultClientSetup implements ClientSetup {
    private final DuplexConnection connection;

    public DefaultClientSetup(DuplexConnection connection) {
      this.connection = connection;
    }

    @Override
    public DuplexConnection connection() {
      return connection;
    }

    @Override
    public KeepAliveHandler keepAliveHandler() {
      return new DefaultKeepAliveHandler();
    }

    @Override
    public ByteBuf resumeToken() {
      return Unpooled.EMPTY_BUFFER;
    }
  }

  class ResumableClientSetup implements ClientSetup {
    private final ByteBuf resumeToken;
    private final ResumableDuplexConnection duplexConnection;
    private final ResumableKeepAliveHandler keepAliveHandler;

    public ResumableClientSetup(
        ByteBufAllocator allocator,
        DuplexConnection connection,
        Mono<DuplexConnection> newConnectionFactory,
        ByteBuf resumeToken,
        ResumableFramesStore resumableFramesStore,
        Duration resumeSessionDuration,
        Duration resumeStreamTimeout,
        Supplier<ResumeStrategy> resumeStrategySupplier,
        boolean cleanupStoreOnKeepAlive) {

      ClientRSocketSession rSocketSession =
          new ClientRSocketSession(
                  connection,
                  allocator,
                  resumeSessionDuration,
                  resumeStrategySupplier,
                  resumableFramesStore,
                  resumeStreamTimeout,
                  cleanupStoreOnKeepAlive)
              .continueWith(newConnectionFactory)
              .resumeToken(resumeToken);
      this.duplexConnection = rSocketSession.resumableConnection();
      this.keepAliveHandler = new ResumableKeepAliveHandler(duplexConnection);
      this.resumeToken = resumeToken;
    }

    @Override
    public DuplexConnection connection() {
      return duplexConnection;
    }

    @Override
    public KeepAliveHandler keepAliveHandler() {
      return keepAliveHandler;
    }

    @Override
    public ByteBuf resumeToken() {
      return resumeToken;
    }
  }
}
