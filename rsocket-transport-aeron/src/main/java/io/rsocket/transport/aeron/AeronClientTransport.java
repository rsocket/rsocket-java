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
import io.aeron.AvailableImageHandler;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class AeronClientTransport implements ClientTransport {
  static final Logger logger = LoggerFactory.getLogger(AeronClientTransport.class);

  static final ThreadLocal<BufferClaim> BUFFER_CLAIM = ThreadLocal.withInitial(BufferClaim::new);

  private final Aeron aeron;
  private final String channel;
  private final int prefetch;
  private final int effort;
  private final long timeoutNs;
  private final Scheduler bossScheduler;
  private final EventLoopGroup eventLoopGroup;
  private final IdleStrategy idleStrategy;
  private final ByteBufAllocator allocator;

  volatile int streamId;
  static final AtomicIntegerFieldUpdater<AeronClientTransport> STREAM_ID =
      AtomicIntegerFieldUpdater.newUpdater(AeronClientTransport.class, "streamId");

  AeronClientTransport(
      Aeron aeron,
      String channel,
      Scheduler bossScheduler,
      EventLoopGroup eventLoopGroup,
      IdleStrategy idleStrategy,
      ByteBufAllocator allocator,
      int prefetch,
      int effort,
      long timeoutNs) {
    this.bossScheduler = bossScheduler;
    this.eventLoopGroup = eventLoopGroup;
    this.idleStrategy = idleStrategy;
    this.allocator = allocator;
    this.aeron = aeron;
    this.channel = channel;
    this.prefetch = prefetch;
    this.effort = effort;
    this.timeoutNs = timeoutNs;

    STREAM_ID.lazySet(this, 2);
  }

  @Override
  public Mono<DuplexConnection> connect() {
    return Mono.defer(
            () -> {
              final long connectionId = ThreadLocalRandom.current().nextInt();
              final int streamId = STREAM_ID.getAndAdd(this, 2);

              if (logger.isDebugEnabled()) {
                logger.debug(
                    "Wiring new connection [{}] for stream [{}] on channel [{}]",
                    connectionId,
                    streamId,
                    channel);
              }

              final ExclusivePublication serverManagementPublication =
                  aeron.addExclusivePublication(channel, Constants.SERVER_MANAGEMENT_STREAM_ID);

              final Subscription clientManagementSubscription =
                  aeron.addSubscription(
                      ChannelUri.addSessionId(channel, (int) connectionId),
                      Constants.CLIENT_MANAGEMENT_STREAM_ID);

              if (!PublicationUtils.tryOfferSetupFrame(
                  aeron.context(),
                  serverManagementPublication,
                  BUFFER_CLAIM.get(),
                  idleStrategy,
                  ChannelUri.addSessionId(channel, (int) connectionId),
                  connectionId,
                  streamId,
                  timeoutNs,
                  FrameType.SETUP)) {

                if (logger.isDebugEnabled()) {
                  logger.debug(
                      "Failed to send SetupFrame { connection: [{}]; stream: [{}]; channel: [{}] }",
                      connectionId,
                      streamId,
                      channel);
                }

                CloseHelper.quietCloseAll(
                    serverManagementPublication, clientManagementSubscription);

                return Mono.error(
                    new TimeoutException(
                        String.format(
                            "Timeout on send SetupFrame { connection: [%s]; stream: [%s]; channel: [%s] }",
                            connectionId, streamId, channel)));
              }

              CloseHelper.quietClose(serverManagementPublication);

              return Mono.<DuplexConnection>create(
                      sink -> {
                        FragmentHandler fragmentHandler =
                            (buffer, offset, length, header) -> {
                              if (PayloadCodec.frameType(buffer, offset) != FrameType.SETUP_COMPLETE
                                  || SetupCodec.connectionId(buffer, offset) != connectionId) {
                                sink.error(new IllegalStateException("Received unexpected frame"));
                                return;
                              }

                              final int serverStreamId = SetupCodec.streamId(buffer, offset);
                              final String serverChannel = SetupCodec.channel(buffer, offset);

                              final ExclusivePublication publication =
                                  aeron.addExclusivePublication(
                                      ChannelUri.addSessionId(serverChannel, (int) connectionId),
                                      serverStreamId);

                              final Subscription subscription =
                                  aeron.addSubscription(
                                      ChannelUri.addSessionId(channel, (int) connectionId),
                                      streamId,
                                      new AvailableImageHandler() {
                                        boolean createdConnection = false;

                                        @Override
                                        public void onAvailableImage(Image image) {
                                          if (!createdConnection) {
                                            createdConnection = true;
                                            sink.success(
                                                new AeronDuplexConnection(
                                                    serverStreamId,
                                                    publication,
                                                    image.subscription(),
                                                    eventLoopGroup,
                                                    allocator,
                                                    prefetch,
                                                    effort));
                                          }
                                        }
                                      },
                                      image -> {});
                              sink.onCancel(
                                  () -> CloseHelper.quietCloseAll(subscription, publication));
                            };

                        final NanoClock nanoClock = aeron.context().nanoClock();
                        final long nowNs = nanoClock.nanoTime();
                        final long deadlineNs = nowNs + timeoutNs;

                        idleStrategy.reset();
                        for (; ; ) {
                          final int polled = clientManagementSubscription.poll(fragmentHandler, 1);

                          if (polled < 1) {
                            idleStrategy.idle();

                            if (deadlineNs - nanoClock.nanoTime() < 0) {
                              sink.error(
                                  new TimeoutException(
                                      "Timeout on waiting response from the AeronServer["
                                          + channel
                                          + "]"));

                              if (logger.isDebugEnabled()) {
                                logger.debug(
                                    "Timeout on receiving SetupCompleteFrame { connection: [{}]; stream: [{}]; channel: [{}] }",
                                    connectionId,
                                    streamId,
                                    channel);
                              }

                              CloseHelper.quietClose(clientManagementSubscription);
                              return;
                            }
                            continue;
                          }

                          CloseHelper.quietClose(clientManagementSubscription);
                          return;
                        }
                      })
                  .timeout(Duration.ofNanos(timeoutNs).multipliedBy(2));
            })
        .subscribeOn(bossScheduler);
  }

  public static AeronClientTransport createUdp(
      Aeron aeron, String host, int port, EventLoopGroup resources) {
    final Supplier<IdleStrategy> idleStrategySupplier =
        () ->
            new BackoffIdleStrategy(
                /* maxSpins */ 100,
                /* maxYields */ 1000,
                /* minParkPeriodNs */ 10000,
                /* maxParkPeriodNs */ 100000);
    return new AeronClientTransport(
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

  public static AeronClientTransport createIpc(Aeron aeron, EventLoopGroup resources) {
    final Supplier<IdleStrategy> idleStrategySupplier =
        () ->
            new BackoffIdleStrategy(
                /* maxSpins */ 100,
                /* maxYields */ 1000,
                /* minParkPeriodNs */ 10000,
                /* maxParkPeriodNs */ 100000);
    return new AeronClientTransport(
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
