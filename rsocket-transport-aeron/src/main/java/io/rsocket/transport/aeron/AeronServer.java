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
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.rsocket.Closeable;
import org.agrona.CloseHelper;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.IdleStrategy;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

public class AeronServer extends BaseSubscriber<Void> implements Closeable {

  final AeronChannelAddress address;
  final Aeron aeron;
  final Scheduler scheduler;
  final Sinks.One<Void> onCloseSink = Sinks.one();

  final Int2ObjectHashMap<AeronDuplexConnection> activeConnections;

  AeronServer(String channel, Aeron aeron, Scheduler scheduler) {
    this.address = new AeronChannelAddress(channel);
    this.aeron = aeron;
    this.scheduler = scheduler;
    this.activeConnections = new Int2ObjectHashMap<>();
  }

  public AeronChannelAddress address() {
    return this.address;
  }

  static Mono<AeronServer> create(
      String channel,
      Aeron aeron,
      Scheduler scheduler,
      IdleStrategy idleStrategy,
      Handler handler) {
    return Mono.fromCallable(
        () -> {
          final AeronServer aeronServer = new AeronServer(channel, aeron, scheduler);
          final Int2ObjectHashMap<AeronDuplexConnection> activeConnections =
              aeronServer.activeConnections;
          return Mono.<Void>create(
                  sink -> {
                    final Subscription serverManagementSubscription =
                        aeron.addSubscription(channel, Constants.SERVER_MANAGEMENT_STREAM_ID);
                    final FragmentHandler fragmentHandler =
                        (buffer, offset, length, header) ->
                            handler
                                .handle(
                                    SetupCodec.connectionId(buffer, offset),
                                    SetupCodec.streamId(buffer, offset),
                                    SetupCodec.channel(buffer, offset))
                                .subscribe(new ConnectionSubscriber(activeConnections));

                    while (!aeronServer.isDisposed() && !serverManagementSubscription.isClosed()) {
                      idleStrategy.idle(
                          serverManagementSubscription.poll(fragmentHandler, Integer.MAX_VALUE));
                    }

                    if (aeronServer.isDisposed()) {
                      serverManagementSubscription.close();

                      synchronized (activeConnections) {
                        activeConnections.forEach((__, c) -> CloseHelper.quietClose(c));
                        activeConnections.clear();
                      }

                      return;
                    }

                    if (serverManagementSubscription.isClosed()) {
                      synchronized (activeConnections) {
                        activeConnections.forEach((__, c) -> CloseHelper.quietClose(c));
                        activeConnections.clear();
                      }

                      sink.error(
                          new NotConnectedException(
                              "Aeron channel[" + channel + "] has been closed"));
                    }
                  })
              .subscribeOn(scheduler)
              .subscribeWith(aeronServer);
        });
  }

  @Override
  public Mono<Void> onClose() {
    return onCloseSink.asMono();
  }

  @Override
  protected void hookFinally(SignalType type) {
    onCloseSink.tryEmitEmpty();
  }

  @FunctionalInterface
  interface Handler {
    Mono<AeronDuplexConnection> handle(long connectionId, int streamId, String channel);
  }

  static class ConnectionSubscriber extends BaseSubscriber<AeronDuplexConnection> {

    final Int2ObjectHashMap<AeronDuplexConnection> activeConnections;

    ConnectionSubscriber(Int2ObjectHashMap<AeronDuplexConnection> connections) {
      activeConnections = connections;
    }

    @Override
    protected void hookOnNext(AeronDuplexConnection connection) {
      final Int2ObjectHashMap<AeronDuplexConnection> activeConnections = this.activeConnections;

      synchronized (activeConnections) {
        if (activeConnections.putIfAbsent(connection.streamId(), connection) != null) {
          connection.dispose();
          return;
        }
      }

      connection
          .onClose()
          .subscribe(
              null,
              __ -> {
                synchronized (activeConnections) {
                  activeConnections.remove(connection.streamId(), connection);
                }
              },
              () -> {
                synchronized (activeConnections) {
                  activeConnections.remove(connection.streamId(), connection);
                }
              });
    }

    @Override
    protected void hookOnError(Throwable throwable) {}
  }
}
