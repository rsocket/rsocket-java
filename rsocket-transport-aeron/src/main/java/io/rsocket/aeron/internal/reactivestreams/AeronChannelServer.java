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
import io.rsocket.Closeable;
import io.rsocket.aeron.internal.AeronWrapper;
import io.rsocket.aeron.internal.Constants;
import io.rsocket.aeron.internal.EventLoop;
import io.rsocket.aeron.internal.NotConnectedException;
import io.rsocket.aeron.internal.reactivestreams.messages.AckConnectEncoder;
import io.rsocket.aeron.internal.reactivestreams.messages.ConnectDecoder;
import io.rsocket.aeron.internal.reactivestreams.messages.MessageHeaderDecoder;
import io.rsocket.aeron.internal.reactivestreams.messages.MessageHeaderEncoder;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Implementation of {@link
 * io.rsocket.aeron.internal.reactivestreams.ReactiveStreamsRemote.ChannelServer} that manages
 * {@link AeronChannel}s.
 */
public class AeronChannelServer
    extends ReactiveStreamsRemote.ChannelServer<AeronChannelServer.AeronChannelConsumer> {
  private static final Logger logger = LoggerFactory.getLogger(AeronChannelServer.class);
  private final AeronWrapper aeronWrapper;
  private final AeronSocketAddress managementSubscriptionSocket;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final ConcurrentHashMap<String, Subscription> serverSubscriptions;
  private volatile boolean running = true;
  private final EventLoop eventLoop;
  private Subscription managementSubscription;
  private AeronChannelStartedServer startServer;

  private AeronChannelServer(
      AeronChannelConsumer channelConsumer,
      AeronWrapper aeronWrapper,
      AeronSocketAddress managementSubscriptionSocket,
      EventLoop eventLoop) {
    super(channelConsumer);
    this.aeronWrapper = aeronWrapper;
    this.managementSubscriptionSocket = managementSubscriptionSocket;
    this.eventLoop = eventLoop;
    this.serverSubscriptions = new ConcurrentHashMap<>();
  }

  public static AeronChannelServer create(
      AeronChannelConsumer channelConsumer,
      AeronWrapper aeronWrapper,
      AeronSocketAddress managementSubscriptionSocket,
      EventLoop eventLoop) {
    return new AeronChannelServer(
        channelConsumer, aeronWrapper, managementSubscriptionSocket, eventLoop);
  }

  @Override
  public AeronChannelStartedServer start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("server already started");
    }

    logger.debug(
        "management server starting on {}, stream id {}",
        managementSubscriptionSocket.getChannel(),
        Constants.SERVER_MANAGEMENT_STREAM_ID);

    this.managementSubscription =
        aeronWrapper.addSubscription(
            managementSubscriptionSocket.getChannel(), Constants.SERVER_MANAGEMENT_STREAM_ID);

    this.startServer = new AeronChannelStartedServer();

    poll();

    return startServer;
  }

  private final FragmentAssembler fragmentAssembler =
      new FragmentAssembler(
          new FragmentHandler() {
            private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
            private final ConnectDecoder connectDecoder = new ConnectDecoder();
            private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
            private final AckConnectEncoder ackConnectEncoder = new AckConnectEncoder();

            @Override
            public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
              messageHeaderDecoder.wrap(buffer, offset);

              // Do not change the order or remove fields
              final int actingBlockLength = messageHeaderDecoder.blockLength();
              final int templateId = messageHeaderDecoder.templateId();
              final int schemaId = messageHeaderDecoder.schemaId();
              final int actingVersion = messageHeaderDecoder.version();

              if (templateId == ConnectDecoder.TEMPLATE_ID) {
                offset += messageHeaderDecoder.encodedLength();
                connectDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);

                // Do not change the order or remove fields
                long channelId = connectDecoder.channelId();
                String receivingChannel = connectDecoder.receivingChannel();
                int receivingStreamId = connectDecoder.receivingStreamId();
                String sendingChannel = connectDecoder.sendingChannel();
                int sendingStreamId = connectDecoder.sendingStreamId();
                int clientSessionId = connectDecoder.clientSessionId();
                String clientManagementChannel = connectDecoder.clientManagementChannel();

                logger.debug(
                    "server creating a AeronChannel with channel id {} receiving on receivingChannel {}, receivingStreamId {}, sendingChannel {}, sendingStreamId {}",
                    channelId,
                    receivingChannel,
                    receivingStreamId,
                    sendingChannel,
                    sendingStreamId);

                // Server sends to receiving Channel
                Publication destination =
                    aeronWrapper.addPublication(receivingChannel, receivingStreamId);
                int sessionId = destination.sessionId();
                logger.debug(
                    "server created publication to channel {}, stream id {}, and session id {}",
                    receivingChannel,
                    receivingStreamId,
                    sessionId);

                // Server listens to sending channel
                Subscription source =
                    serverSubscriptions.computeIfAbsent(
                        sendingChannel,
                        s -> aeronWrapper.addSubscription(sendingChannel, sendingStreamId));
                logger.debug(
                    "server created subscription to channel {}, stream id {}",
                    sendingChannel,
                    sendingStreamId);

                AeronChannel aeronChannel =
                    new AeronChannel("server", destination, source, eventLoop, clientSessionId);
                logger.debug(
                    "server create AeronChannel with destination channel {}, source channel {}, and clientSessionId {}");

                channelConsumer.accept(aeronChannel);

                Publication managementPublication =
                    aeronWrapper.addPublication(
                        clientManagementChannel, Constants.CLIENT_MANAGEMENT_STREAM_ID);
                logger.debug(
                    "server created management publication to channel {}", clientManagementChannel);

                final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);
                final UnsafeBuffer directBuffer = new UnsafeBuffer(byteBuffer);
                int bufferOffset = 0;

                messageHeaderEncoder
                    .wrap(directBuffer, bufferOffset)
                    .blockLength(AckConnectEncoder.BLOCK_LENGTH)
                    .templateId(AckConnectEncoder.TEMPLATE_ID)
                    .schemaId(AckConnectEncoder.SCHEMA_ID)
                    .version(AckConnectEncoder.SCHEMA_VERSION);

                bufferOffset += messageHeaderEncoder.encodedLength();

                ackConnectEncoder
                    .wrap(directBuffer, bufferOffset)
                    .channelId(channelId)
                    .serverSessionId(destination.sessionId());

                logger.debug(
                    "server sending AckConnect message to channel {}", clientManagementChannel);

                long offer;
                do {
                  offer = managementPublication.offer(directBuffer);
                  if (offer == Publication.CLOSED) {
                    throw new NotConnectedException();
                  }
                } while (offer < 0);
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

  public interface AeronChannelConsumer
      extends ReactiveStreamsRemote.ChannelConsumer<AeronChannel> {}

  public class AeronChannelStartedServer implements ReactiveStreamsRemote.StartedServer, Closeable {
    private final MonoProcessor<Void> onClose = MonoProcessor.create();

    public AeronWrapper getAeronWrapper() {
      return aeronWrapper;
    }

    public EventLoop getEventLoop() {
      return eventLoop;
    }

    @Override
    public SocketAddress getServerAddress() {
      return managementSubscriptionSocket;
    }

    @Override
    public int getServerPort() {
      return managementSubscriptionSocket.getPort();
    }

    @Override
    public void awaitShutdown(long duration, TimeUnit durationUnit) {
      Duration d = Duration.ofMillis(durationUnit.toMillis(duration));
      onClose().block(d);
    }

    @Override
    public void awaitShutdown() {
      onClose().block();
    }

    @Override
    public void shutdown() {
      dispose();
    }

    @Override
    public void dispose() {
      running = false;
      managementSubscription.close();
      onClose.onComplete();
    }

    @Override
    public boolean isDisposed() {
      return onClose.isDisposed();
    }

    @Override
    public Mono<Void> onClose() {
      return onClose;
    }
  }
}
