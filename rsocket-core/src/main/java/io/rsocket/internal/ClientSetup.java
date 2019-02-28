package io.rsocket.internal;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.keepalive.KeepAliveConnection;
import io.rsocket.resume.ClientRSocketSession;
import io.rsocket.resume.ClientResumeConfiguration;
import io.rsocket.resume.ResumeStrategy;
import io.rsocket.resume.ResumeToken;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Supplier;

public interface ClientSetup {
  /*Provide different connections for SETUP / RESUME cases*/
  DuplexConnection wrappedConnection(KeepAliveConnection duplexConnection);

  /*Provide different resume tokens for SETUP / RESUME cases*/
  ResumeToken resumeToken();

  class DefaultClientSetup implements ClientSetup {

    @Override
    public DuplexConnection wrappedConnection(KeepAliveConnection connection) {
      return connection;
    }

    @Override
    public ResumeToken resumeToken() {
      return ResumeToken.empty();
    }
  }

  class ResumableClientSetup implements ClientSetup {
    private final ResumeToken resumeToken;
    private final ClientResumeConfiguration config;
    private final ByteBufAllocator allocator;
    private final Mono<KeepAliveConnection> newConnection;

    public ResumableClientSetup(
            ByteBufAllocator allocator,
            Mono<KeepAliveConnection> newConnection,
            ResumeToken resumeToken,
            int resumeCacheSize,
            Duration resumeSessionDuration,
            Supplier<ResumeStrategy> resumeStrategySupplier) {
      this.allocator = allocator;
      this.newConnection = newConnection;
      this.resumeToken = resumeToken;
      this.config =
          new ClientResumeConfiguration(
              resumeSessionDuration, resumeCacheSize, resumeStrategySupplier);
    }

    @Override
    public DuplexConnection wrappedConnection(KeepAliveConnection connection) {
      ClientRSocketSession rSocketSession =
          new ClientRSocketSession(allocator, connection, config)
              .continueWith(newConnection)
              .resumeWith(resumeToken);

      return rSocketSession.resumableConnection();
    }

    @Override
    public ResumeToken resumeToken() {
      return resumeToken;
    }
  }
}
