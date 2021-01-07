/*
 * Copyright 2015-present the original author or authors.
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
package io.rsocket.transport.aeron;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.logbuffer.BufferClaim;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.transport.ServerTransport;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class AeronServerTransport implements ServerTransport<AeronServer> {

  static final Logger logger = LoggerFactory.getLogger(AeronServerTransport.class);

  static final ThreadLocal<BufferClaim> BUFFER_CLAIM = ThreadLocal.withInitial(BufferClaim::new);

  private final Scheduler bossScheduler;

  private final EventLoopGroup eventLoopGroup;
  private final Aeron aeron;
  private final String channel;
  private final IdleStrategy idleStrategy;
  private final ByteBufAllocator allocator;
  private final int prefetch;
  private final int effort;
  private final long timeoutNs;

  AeronServerTransport(
      Aeron aeron,
      String channel,
      Scheduler bossScheduler,
      EventLoopGroup eventLoopGroup,
      IdleStrategy idleStrategy,
      ByteBufAllocator allocator,
      int prefetch,
      int effort,
      long timeoutNs) {
    this.eventLoopGroup = eventLoopGroup;
    this.bossScheduler = bossScheduler;
    this.aeron = aeron;
    this.channel = channel;
    this.idleStrategy = idleStrategy;
    this.allocator = allocator;
    this.prefetch = prefetch;
    this.effort = effort;
    this.timeoutNs = timeoutNs;
  }

  @Override
  public Mono<AeronServer> start(ConnectionAcceptor acceptor) {
    return AeronServer.create(
        channel, aeron, bossScheduler, idleStrategy, new AeronConnectionHandler(acceptor));
  }

  class AeronConnectionHandler implements AeronServer.Handler {

    final ConnectionAcceptor acceptor;

    int serverStreamId = -1;

    AeronConnectionHandler(ConnectionAcceptor acceptor) {
      this.acceptor = acceptor;
    }

    @Override
    public Mono<AeronDuplexConnection> handle(
        long clientConnectionId, int clientStreamId, String clientChannel) {
      final int serverStreamId = this.serverStreamId + 2;
      this.serverStreamId = serverStreamId;

      logger.info(
          "Receiving connection with id {} from aeron stream id {} and channel {}",
          clientConnectionId,
          clientStreamId,
          clientChannel);

      final ExclusivePublication clientManagementPublication;
      try {
        clientManagementPublication =
            aeron.addExclusivePublication(
                ChannelUri.addSessionId(clientChannel, (int) clientConnectionId),
                Constants.CLIENT_MANAGEMENT_STREAM_ID);
      } catch (Throwable t) {
        return Mono.error(t);
      }

      final ExclusivePublication clientPublication;
      try {
        clientPublication =
            aeron.addExclusivePublication(
                ChannelUri.addSessionId(clientChannel, (int) clientConnectionId), clientStreamId);
      } catch (Throwable t) {
        return Mono.error(t);
      }

      final Subscription subscription;
      try {
        subscription =
            aeron.addSubscription(
                ChannelUri.addSessionId(channel, (int) clientConnectionId),
                serverStreamId,
                image -> {
                  logger.debug("receive image");
                  eventLoopGroup.next().schedule(clientManagementPublication::close);
                },
                image -> {});
      } catch (Throwable t) {
        return Mono.error(t);
      }

      final AeronDuplexConnection connection =
          new AeronDuplexConnection(
              serverStreamId,
              clientPublication,
              subscription,
              eventLoopGroup,
              allocator,
              prefetch,
              effort);

      logger.info(
          "received connection [{}] from aeron channel [{}]", clientConnectionId, clientChannel);

      final BufferClaim bufferClaim = BUFFER_CLAIM.get();
      if (!PublicationUtils.tryOfferSetupFrame(
          aeron.context(),
          clientManagementPublication,
          bufferClaim,
          idleStrategy,
          ChannelUri.addSessionId(channel, (int) clientConnectionId),
          clientConnectionId,
          serverStreamId,
          timeoutNs,
          FrameType.SETUP_COMPLETE)) {

        this.serverStreamId -= 2;

        logger.debug(
            "Failed to send SetupCompleteFrame { connection: [{}]; stream: [{}]; channel: [{}] }",
            clientConnectionId,
            clientStreamId,
            clientChannel);

        CloseHelper.quietCloseAll(clientManagementPublication, connection);

        return Mono.error(
            new TimeoutException(
                "Timeout on send SetupCompleteFrame { connection: [{}]; stream: [{}]; channel: [{}] }"));
      }

      return acceptor
          .apply(connection)
          .thenReturn(connection)
          .timeout(Duration.ofNanos(timeoutNs))
          .doOnError(
              cause -> {
                logger.debug(
                    "Cleanup connection [{}] resource. Cause: {}", clientConnectionId, cause);
                CloseHelper.quietCloseAll(clientManagementPublication, connection);
              });
    }
  }

  public static AeronServerTransport createUdp(
      Aeron aeron, String host, int port, EventLoopGroup resources) {
    final Supplier<IdleStrategy> idleStrategySupplier =
        () -> new BackoffIdleStrategy(100, 1000, 10000, 100000);
    return new AeronServerTransport(
        aeron,
        new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .endpoint(host + ":" + port)
            .build(),
        Schedulers.boundedElastic(),
        resources,
        idleStrategySupplier.get(),
        ByteBufAllocator.DEFAULT,
        256,
        256,
        Duration.ofSeconds(5).toNanos());
  }

  public static AeronServerTransport createIpc(Aeron aeron, EventLoopGroup resources) {
    final Supplier<IdleStrategy> idleStrategySupplier =
        () -> new BackoffIdleStrategy(100, 1000, 10000, 100000);
    return new AeronServerTransport(
        aeron,
        new ChannelUriStringBuilder().media(CommonContext.IPC_MEDIA).build(),
        Schedulers.boundedElastic(),
        resources,
        idleStrategySupplier.get(),
        ByteBufAllocator.DEFAULT,
        256,
        256,
        Duration.ofSeconds(5).toNanos());
  }
}
