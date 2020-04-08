/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.lease.LeaseStats;
import io.rsocket.lease.Leases;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.plugins.SocketAcceptorInterceptor;
import io.rsocket.resume.ResumableFramesStore;
import io.rsocket.resume.ResumeStrategy;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Main entry point to create RSocket clients or servers as follows:
 *
 * <ul>
 *   <li>{@link ClientRSocketFactory} to connect as a client. Use {@link #connect()} for a default
 *       instance.
 *   <li>{@link ServerRSocketFactory} to start a server. Use {@link #receive()} for a default
 *       instance.
 * </ul>
 */
public final class RSocketFactory {

  private static final Constructor<?> clientFactoryConstructor =
      getConstructorFor("io.rsocket.core.DefaultClientRSocketFactory");

  private static final Constructor<?> serverFactoryConstructor =
      getConstructorFor("io.rsocket.core.DefaultServerRSocketFactory");

  /**
   * Create a {@link ClientRSocketFactory} to connect to a remote RSocket endpoint. A shortcut for
   * creating {@link io.rsocket.core.DefaultClientRSocketFactory}.
   *
   * @return the {@code ClientRSocketFactory} instance
   */
  public static ClientRSocketFactory connect() {
    try {
      // Avoid explicit dependency and a package cycle
      return (ClientRSocketFactory) clientFactoryConstructor.newInstance();
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to create ClientRSocketFactory", ex);
    }
  }

  /**
   * Create a {@link ServerRSocketFactory} to accept connections from RSocket clients. A shortcut
   * for creating {@link io.rsocket.core.DefaultServerRSocketFactory}.
   *
   * @return the {@code ClientRSocketFactory} instance
   */
  public static ServerRSocketFactory receive() {
    try {
      // Avoid explicit dependency and a package cycle
      return (ServerRSocketFactory) serverFactoryConstructor.newInstance();
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to create ServerRSocketFactory", ex);
    }
  }
  /** Factory to create and configure an RSocket client, and connect to a server. */
  public interface ClientRSocketFactory extends ClientTransportAcceptor {

    ClientRSocketFactory byteBufAllocator(ByteBufAllocator allocator);

    ClientRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor);

    @Deprecated
    ClientRSocketFactory addClientPlugin(RSocketInterceptor interceptor);

    ClientRSocketFactory addRequesterPlugin(RSocketInterceptor interceptor);

    @Deprecated
    ClientRSocketFactory addServerPlugin(RSocketInterceptor interceptor);

    ClientRSocketFactory addResponderPlugin(RSocketInterceptor interceptor);

    ClientRSocketFactory addSocketAcceptorPlugin(SocketAcceptorInterceptor interceptor);

    /**
     * Deprecated as Keep-Alive is not optional according to spec
     *
     * @return this ClientRSocketFactory
     */
    @Deprecated
    ClientRSocketFactory keepAlive();

    ClientRSocketFactory keepAlive(Duration tickPeriod, Duration ackTimeout, int missedAcks);

    ClientRSocketFactory keepAliveTickPeriod(Duration tickPeriod);

    ClientRSocketFactory keepAliveAckTimeout(Duration ackTimeout);

    ClientRSocketFactory keepAliveMissedAcks(int missedAcks);

    ClientRSocketFactory mimeType(String metadataMimeType, String dataMimeType);

    ClientRSocketFactory dataMimeType(String dataMimeType);

    ClientRSocketFactory metadataMimeType(String metadataMimeType);

    ClientRSocketFactory lease(Supplier<Leases<? extends LeaseStats>> leasesSupplier);

    ClientRSocketFactory lease();

    ClientRSocketFactory singleSubscriberRequester();

    /**
     * Enables a reconnectable, shared instance of {@code Mono<RSocket>} so every subscriber will
     * observe the same RSocket instance up on connection establishment. <br>
     * For example:
     *
     * <pre>{@code
     * Mono<RSocket> sharedRSocketMono =
     *   RSocketFactory
     *                .connect()
     *                .singleSubscriberRequester()
     *                .reconnect(Retry.fixedDelay(3, Duration.ofSeconds(1)))
     *                .transport(transport)
     *                .start();
     *
     *  RSocket r1 = sharedRSocketMono.block();
     *  RSocket r2 = sharedRSocketMono.block();
     *
     *  assert r1 == r2;
     *
     * }</pre>
     *
     * Apart of the shared behavior, if the connection is lost, the same {@code Mono<RSocket>}
     * instance will transparently re-establish the connection for subsequent subscribers.<br>
     * For example:
     *
     * <pre>{@code
     * Mono<RSocket> sharedRSocketMono =
     *   RSocketFactory
     *                .connect()
     *                .singleSubscriberRequester()
     *                .reconnect(Retry.fixedDelay(3, Duration.ofSeconds(1)))
     *                .transport(transport)
     *                .start();
     *
     *  RSocket r1 = sharedRSocketMono.block();
     *  RSocket r2 = sharedRSocketMono.block();
     *
     *  assert r1 == r2;
     *
     *  r1.dispose()
     *
     *  assert r2.isDisposed()
     *
     *  RSocket r3 = sharedRSocketMono.block();
     *  RSocket r4 = sharedRSocketMono.block();
     *
     *
     *  assert r1 != r3;
     *  assert r4 == r3;
     *
     * }</pre>
     *
     * <b>Note,</b> having reconnect() enabled does not eliminate the need to accompany each
     * individual request with the corresponding retry logic. <br>
     * For example:
     *
     * <pre>{@code
     * Mono<RSocket> sharedRSocketMono =
     *   RSocketFactory
     *                .connect()
     *                .singleSubscriberRequester()
     *                .reconnect(Retry.fixedDelay(3, Duration.ofSeconds(1)))
     *                .transport(transport)
     *                .start();
     *
     *  sharedRSocket.flatMap(rSocket -> rSocket.requestResponse(...))
     *               .retryWhen(ownRetry)
     *               .subscribe()
     *
     * }</pre>
     *
     * @param retrySpec a retry factory applied for {@link Mono#retryWhen(Retry)}
     * @return a shared instance of {@code Mono<RSocket>}.
     */
    ClientRSocketFactory reconnect(Retry retrySpec);

    ClientRSocketFactory resume();

    ClientRSocketFactory resumeToken(Supplier<ByteBuf> resumeTokenSupplier);

    ClientRSocketFactory resumeStore(
        Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory);

    ClientRSocketFactory resumeSessionDuration(Duration sessionDuration);

    ClientRSocketFactory resumeStreamTimeout(Duration resumeStreamTimeout);

    ClientRSocketFactory resumeStrategy(Supplier<ResumeStrategy> resumeStrategy);

    ClientRSocketFactory resumeCleanupOnKeepAlive();

    @Override
    Start<RSocket> transport(Supplier<ClientTransport> transportClient);

    ClientTransportAcceptor acceptor(Function<RSocket, RSocket> acceptor);

    ClientTransportAcceptor acceptor(Supplier<Function<RSocket, RSocket>> acceptor);

    ClientTransportAcceptor acceptor(SocketAcceptor acceptor);

    ClientRSocketFactory fragment(int mtu);

    ClientRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer);

    ClientRSocketFactory setupPayload(Payload payload);

    ClientRSocketFactory frameDecoder(PayloadDecoder payloadDecoder);
  }

  /** Factory to create, configure, and start an RSocket server. */
  public interface ServerRSocketFactory {
    ServerRSocketFactory byteBufAllocator(ByteBufAllocator allocator);

    ServerRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor);

    @Deprecated
    ServerRSocketFactory addClientPlugin(RSocketInterceptor interceptor);

    ServerRSocketFactory addRequesterPlugin(RSocketInterceptor interceptor);

    @Deprecated
    ServerRSocketFactory addServerPlugin(RSocketInterceptor interceptor);

    ServerRSocketFactory addResponderPlugin(RSocketInterceptor interceptor);

    ServerRSocketFactory addSocketAcceptorPlugin(SocketAcceptorInterceptor interceptor);

    ServerTransportAcceptor acceptor(SocketAcceptor acceptor);

    ServerRSocketFactory frameDecoder(PayloadDecoder payloadDecoder);

    ServerRSocketFactory fragment(int mtu);

    ServerRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer);

    ServerRSocketFactory lease(Supplier<Leases<?>> leasesSupplier);

    ServerRSocketFactory lease();

    ServerRSocketFactory singleSubscriberRequester();

    ServerRSocketFactory resume();

    ServerRSocketFactory resumeStore(
        Function<? super ByteBuf, ? extends ResumableFramesStore> resumeStoreFactory);

    ServerRSocketFactory resumeSessionDuration(Duration sessionDuration);

    ServerRSocketFactory resumeStreamTimeout(Duration resumeStreamTimeout);

    ServerRSocketFactory resumeCleanupOnKeepAlive();
  }

  public interface ClientTransportAcceptor {
    Start<RSocket> transport(Supplier<ClientTransport> transport);

    default Start<RSocket> transport(ClientTransport transport) {
      return transport(() -> transport);
    }
  }

  public interface ServerTransportAcceptor {

    ServerTransport.ConnectionAcceptor toConnectionAcceptor();

    <T extends Closeable> Start<T> transport(Supplier<ServerTransport<T>> transport);

    default <T extends Closeable> Start<T> transport(ServerTransport<T> transport) {
      return transport(() -> transport);
    }
  }

  public interface Start<T extends Closeable> {
    Mono<T> start();
  }

  private static Constructor<?> getConstructorFor(String className) {
    try {
      Class<?> clazz = Class.forName(className);
      return clazz.getDeclaredConstructor();
    } catch (Throwable ex) {
      throw new IllegalStateException("No " + className);
    }
  }
}
