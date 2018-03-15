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

package io.rsocket.aeron.internal.reactivestreams;

import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.rsocket.aeron.internal.AeronWrapper;
import io.rsocket.aeron.internal.Constants;
import io.rsocket.aeron.internal.EventLoop;
import io.rsocket.aeron.internal.NotConnectedException;
import io.rsocket.aeron.internal.reactivestreams.messages.AckConnectDecoder;
import io.rsocket.aeron.internal.reactivestreams.messages.ConnectEncoder;
import io.rsocket.aeron.internal.reactivestreams.messages.MessageHeaderDecoder;
import io.rsocket.aeron.internal.reactivestreams.messages.MessageHeaderEncoder;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

/** Brokers a connection to a remote Aeron server. */
public class AeronClientChannelConnector
    implements ReactiveStreamsRemote.ClientChannelConnector<
            AeronClientChannelConnector.AeronClientConfig, AeronChannel>,
        AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(AeronClientChannelConnector.class);

  private static final AtomicLong CHANNEL_ID_COUNTER = new AtomicLong();

  private final AeronWrapper aeronWrapper;

  // Subscriptions clients listen to responses on
  private final ConcurrentHashMap<AeronSocketAddress, Subscription> clientSubscriptions;
  private final ConcurrentHashMap<Long, IntConsumer> serverSessionIdConsumerMap;

  private final Subscription managementSubscription;

  private final EventLoop eventLoop;

  private volatile boolean running = true;

  private AeronClientChannelConnector(
      AeronWrapper aeronWrapper,
      AeronSocketAddress managementSubscriptionSocket,
      EventLoop eventLoop) {
    this.aeronWrapper = aeronWrapper;

    logger.debug(
        "client creating a management subscription on channel {}, stream id {}",
        managementSubscriptionSocket.getChannel(),
        Constants.CLIENT_MANAGEMENT_STREAM_ID);

    this.managementSubscription =
        aeronWrapper.addSubscription(
            managementSubscriptionSocket.getChannel(), Constants.CLIENT_MANAGEMENT_STREAM_ID);
    this.eventLoop = eventLoop;
    this.clientSubscriptions = new ConcurrentHashMap<>();
    this.serverSessionIdConsumerMap = new ConcurrentHashMap<>();

    poll();
  }

  public static AeronClientChannelConnector create(
      AeronWrapper wrapper, AeronSocketAddress managementSubscriptionSocket, EventLoop eventLoop) {
    return new AeronClientChannelConnector(wrapper, managementSubscriptionSocket, eventLoop);
  }

  private final FragmentAssembler fragmentAssembler =
      new FragmentAssembler(
          new FragmentHandler() {
            private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
            private final AckConnectDecoder ackConnectDecoder = new AckConnectDecoder();

            @Override
            public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
              messageHeaderDecoder.wrap(buffer, offset);

              // Do not change the order or remove fields
              final int actingBlockLength = messageHeaderDecoder.blockLength();
              final int templateId = messageHeaderDecoder.templateId();
              final int schemaId = messageHeaderDecoder.schemaId();
              final int actingVersion = messageHeaderDecoder.version();

              if (templateId == AckConnectDecoder.TEMPLATE_ID) {
                logger.debug("client received an ack message on session id {}", header.sessionId());
                offset += messageHeaderDecoder.encodedLength();
                ackConnectDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
                long channelId = ackConnectDecoder.channelId();
                int serverSessionId = ackConnectDecoder.serverSessionId();

                logger.debug(
                    "client received ack message for channel id {} and server session id {}",
                    channelId,
                    serverSessionId);

                IntConsumer intConsumer = serverSessionIdConsumerMap.remove(channelId);

                if (intConsumer != null) {
                  intConsumer.accept(serverSessionId);
                } else {
                  throw new IllegalStateException("no channel found for channel id " + channelId);
                }
              } else {
                throw new IllegalStateException("received unknown template id " + templateId);
              }
            }
          });

  private int poll() {
    int poll;
    try {
      poll = managementSubscription.poll(fragmentAssembler, 4096);
    } finally {
      if (running) {
        boolean execute = eventLoop.execute(this::poll);
        if (!execute) {
          running = false;
          throw new IllegalStateException("unable to keep polling, eventLoop rejection");
        }
      }
    }

    return poll;
  }

  @Override
  public Mono<AeronChannel> apply(AeronClientConfig aeronClientConfig) {
    return Mono.from(
        subscriber -> {
          subscriber.onSubscribe(Operators.emptySubscription());
          final long channelId = CHANNEL_ID_COUNTER.get();
          try {

            logger.debug("Creating new client channel with id {}", channelId);
            final Publication destination =
                aeronWrapper.addPublication(
                    aeronClientConfig.sendSocketAddress.getChannel(),
                    aeronClientConfig.sendStreamId);
            int destinationStreamId = destination.streamId();

            logger.debug(
                "Client created publication to {}, on stream id {}, and session id {}",
                aeronClientConfig.sendSocketAddress,
                aeronClientConfig.sendStreamId,
                destination.sessionId());

            final Subscription source =
                clientSubscriptions.computeIfAbsent(
                    aeronClientConfig.receiveSocketAddress,
                    address -> {
                      Subscription subscription =
                          aeronWrapper.addSubscription(
                              aeronClientConfig.receiveSocketAddress.getChannel(),
                              aeronClientConfig.receiveStreamId);
                      logger.debug(
                          "Client created subscription to {}, on stream id {}",
                          aeronClientConfig.receiveSocketAddress,
                          aeronClientConfig.receiveStreamId);
                      return subscription;
                    });

            IntConsumer sessionIdConsumer =
                sessionId -> {
                  try {
                    AeronChannel aeronChannel =
                        new AeronChannel(
                            "client", destination, source, aeronClientConfig.eventLoop, sessionId);
                    logger.debug(
                        "created client AeronChannel for destination {}, source {}, destination stream id {}, source stream id {}, client session id, and server session id {}",
                        aeronClientConfig.sendSocketAddress,
                        aeronClientConfig.receiveSocketAddress,
                        destination.streamId(),
                        source.streamId(),
                        destination.sessionId(),
                        sessionId);
                    subscriber.onNext(aeronChannel);
                    subscriber.onComplete();
                  } catch (Throwable t) {
                    subscriber.onError(t);
                  }
                };

            serverSessionIdConsumerMap.putIfAbsent(channelId, sessionIdConsumer);

            aeronWrapper.unavailableImageHandlers(
                image -> {
                  if (destinationStreamId == image.sessionId()) {
                    clientSubscriptions.remove(aeronClientConfig.receiveSocketAddress);
                    return true;
                  } else {
                    return false;
                  }
                });

            Publication managementPublication =
                aeronWrapper.addPublication(
                    aeronClientConfig.sendSocketAddress.getChannel(),
                    Constants.SERVER_MANAGEMENT_STREAM_ID);
            logger.debug(
                "Client created management publication to channel {}, stream id {}",
                managementPublication.channel(),
                managementPublication.streamId());

            DirectBuffer buffer =
                encodeConnectMessage(channelId, aeronClientConfig, destination.sessionId());
            long offer;
            do {
              offer = managementPublication.offer(buffer);
              if (offer == Publication.CLOSED) {
                subscriber.onError(new NotConnectedException());
              }
            } while (offer < 0);
            logger.debug("Client sent create message to {}", managementPublication.channel());

          } catch (Throwable t) {
            logger.error("Error creating a channel to {}", aeronClientConfig);
            clientSubscriptions.remove(aeronClientConfig.receiveSocketAddress);
            subscriber.onError(t);
          }
        });
  }

  public DirectBuffer encodeConnectMessage(
      long channelId, AeronClientConfig config, int clientSessionId) {
    final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
    final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);
    int bufferOffset = 0;

    MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

    // Do not channel the order
    messageHeaderEncoder
        .wrap(directBuffer, bufferOffset)
        .blockLength(ConnectEncoder.BLOCK_LENGTH)
        .templateId(ConnectEncoder.TEMPLATE_ID)
        .schemaId(ConnectEncoder.SCHEMA_ID)
        .version(ConnectEncoder.SCHEMA_VERSION);

    bufferOffset += messageHeaderEncoder.encodedLength();

    ConnectEncoder connectEncoder = new ConnectEncoder();

    // Do not change the order
    connectEncoder
        .wrap(directBuffer, bufferOffset)
        .channelId(channelId)
        .receivingChannel(config.receiveSocketAddress.getChannel())
        .receivingStreamId(config.receiveStreamId)
        .sendingChannel(config.sendSocketAddress.getChannel())
        .sendingStreamId(config.sendStreamId)
        .clientSessionId(clientSessionId)
        .clientManagementChannel(managementSubscription.channel());

    return directBuffer;
  }

  public static class AeronClientConfig implements ReactiveStreamsRemote.ClientChannelConfig {
    private final AeronSocketAddress receiveSocketAddress;
    private final AeronSocketAddress sendSocketAddress;
    private final int receiveStreamId;
    private final int sendStreamId;
    private final EventLoop eventLoop;

    private AeronClientConfig(
        AeronSocketAddress receiveSocketAddress,
        AeronSocketAddress sendSocketAddress,
        int receiveStreamId,
        int sendStreamId,
        EventLoop eventLoop) {
      this.receiveSocketAddress = receiveSocketAddress;
      this.sendSocketAddress = sendSocketAddress;
      this.receiveStreamId = receiveStreamId;
      this.sendStreamId = sendStreamId;
      this.eventLoop = eventLoop;
    }

    /**
     * Creates client a new {@code AeronClientConfig} for a {@link AeronChannel}
     *
     * @param receiveSocketAddress the address the channels receives data on
     * @param sendSocketAddress the address the channel sends data too
     * @param receiveStreamId receiving stream id
     * @param sendStreamId the sending stream id
     * @param eventLoop event loop for this client
     * @return new {@code AeronClientConfig}
     */
    public static AeronClientConfig create(
        AeronSocketAddress receiveSocketAddress,
        AeronSocketAddress sendSocketAddress,
        int receiveStreamId,
        int sendStreamId,
        EventLoop eventLoop) {
      return new AeronClientConfig(
          receiveSocketAddress, sendSocketAddress, receiveStreamId, sendStreamId, eventLoop);
    }

    @Override
    public String toString() {
      return "AeronClientConfig{"
          + "receiveSocketAddress="
          + receiveSocketAddress
          + ", sendSocketAddress="
          + sendSocketAddress
          + ", receiveStreamId="
          + receiveStreamId
          + ", sendStreamId="
          + sendStreamId
          + ", eventLoop="
          + eventLoop
          + '}';
    }
  }

  @Override
  public void close() {
    running = false;
  }
}
