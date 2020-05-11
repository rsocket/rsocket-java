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
import io.netty.buffer.Unpooled;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.lease.LeaseStats;
import io.rsocket.lease.Leases;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.plugins.SocketAcceptorInterceptor;
import io.rsocket.resume.ClientResume;
import io.rsocket.resume.ResumableFramesStore;
import io.rsocket.resume.ResumeStrategy;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
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
 *
 * @deprecated please use {@link RSocketConnector} and {@link RSocketServer}.
 */
@Deprecated
public final class RSocketFactory {

  /**
   * Create a {@code ClientRSocketFactory} to connect to a remote RSocket endpoint. Internally
   * delegates to {@link RSocketConnector}.
   *
   * @return the {@code ClientRSocketFactory} instance
   */
  public static ClientRSocketFactory connect() {
    return new ClientRSocketFactory();
  }

  /**
   * Create a {@code ServerRSocketFactory} to accept connections from RSocket clients. Internally
   * delegates to {@link RSocketServer}.
   *
   * @return the {@code ClientRSocketFactory} instance
   */
  public static ServerRSocketFactory receive() {
    return new ServerRSocketFactory();
  }

  public interface Start<T extends Closeable> {
    Mono<T> start();
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

  /** Factory to create and configure an RSocket client, and connect to a server. */
  public static class ClientRSocketFactory implements ClientTransportAcceptor {
    private static final ClientResume CLIENT_RESUME =
        new ClientResume(Duration.ofMinutes(2), Unpooled.EMPTY_BUFFER);

    private final RSocketConnector connector;

    private Duration tickPeriod = Duration.ofSeconds(20);
    private Duration ackTimeout = Duration.ofSeconds(30);
    private int missedAcks = 3;

    private Resume resume;

    public ClientRSocketFactory() {
      this(RSocketConnector.create());
    }

    public ClientRSocketFactory(RSocketConnector connector) {
      this.connector = connector;
    }

    /**
     * @deprecated this method is deprecated and deliberately has no effect anymore. Right now, in
     *     order configure the custom {@link ByteBufAllocator} it is recommended to use the
     *     following setup for Reactor Netty based transport: <br>
     *     1. For Client: <br>
     *     <pre>{@code
     * TcpClient.create()
     *          ...
     *          .bootstrap(bootstrap -> bootstrap.option(ChannelOption.ALLOCATOR, clientAllocator))
     * }</pre>
     *     <br>
     *     2. For server: <br>
     *     <pre>{@code
     * TcpServer.create()
     *          ...
     *          .bootstrap(serverBootstrap -> serverBootstrap.childOption(ChannelOption.ALLOCATOR, serverAllocator))
     * }</pre>
     *     Or in case of local transport, to use corresponding factory method {@code
     *     LocalClientTransport.creat(String, ByteBufAllocator)}
     * @param allocator instance of {@link ByteBufAllocator}
     * @return this factory instance
     */
    public ClientRSocketFactory byteBufAllocator(ByteBufAllocator allocator) {
      return this;
    }

    public ClientRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      connector.interceptors(registry -> registry.forConnection(interceptor));
      return this;
    }

    /** Deprecated. Use {@link #addRequesterPlugin(RSocketInterceptor)} instead */
    @Deprecated
    public ClientRSocketFactory addClientPlugin(RSocketInterceptor interceptor) {
      return addRequesterPlugin(interceptor);
    }

    public ClientRSocketFactory addRequesterPlugin(RSocketInterceptor interceptor) {
      connector.interceptors(registry -> registry.forRequester(interceptor));
      return this;
    }

    /** Deprecated. Use {@link #addResponderPlugin(RSocketInterceptor)} instead */
    @Deprecated
    public ClientRSocketFactory addServerPlugin(RSocketInterceptor interceptor) {
      return addResponderPlugin(interceptor);
    }

    public ClientRSocketFactory addResponderPlugin(RSocketInterceptor interceptor) {
      connector.interceptors(registry -> registry.forResponder(interceptor));
      return this;
    }

    public ClientRSocketFactory addSocketAcceptorPlugin(SocketAcceptorInterceptor interceptor) {
      connector.interceptors(registry -> registry.forSocketAcceptor(interceptor));
      return this;
    }

    /**
     * Deprecated without replacement as Keep-Alive is not optional according to spec
     *
     * @return this ClientRSocketFactory
     */
    @Deprecated
    public ClientRSocketFactory keepAlive() {
      connector.keepAlive(tickPeriod, ackTimeout.plus(tickPeriod.multipliedBy(missedAcks)));
      return this;
    }

    public ClientTransportAcceptor keepAlive(
        Duration tickPeriod, Duration ackTimeout, int missedAcks) {
      this.tickPeriod = tickPeriod;
      this.ackTimeout = ackTimeout;
      this.missedAcks = missedAcks;
      keepAlive();
      return this;
    }

    public ClientRSocketFactory keepAliveTickPeriod(Duration tickPeriod) {
      this.tickPeriod = tickPeriod;
      keepAlive();
      return this;
    }

    public ClientRSocketFactory keepAliveAckTimeout(Duration ackTimeout) {
      this.ackTimeout = ackTimeout;
      keepAlive();
      return this;
    }

    public ClientRSocketFactory keepAliveMissedAcks(int missedAcks) {
      this.missedAcks = missedAcks;
      keepAlive();
      return this;
    }

    public ClientRSocketFactory mimeType(String metadataMimeType, String dataMimeType) {
      connector.metadataMimeType(metadataMimeType);
      connector.dataMimeType(dataMimeType);
      return this;
    }

    public ClientRSocketFactory dataMimeType(String dataMimeType) {
      connector.dataMimeType(dataMimeType);
      return this;
    }

    public ClientRSocketFactory metadataMimeType(String metadataMimeType) {
      connector.metadataMimeType(metadataMimeType);
      return this;
    }

    public ClientRSocketFactory lease(Supplier<Leases<? extends LeaseStats>> supplier) {
      connector.lease(supplier);
      return this;
    }

    public ClientRSocketFactory lease() {
      connector.lease(Leases::new);
      return this;
    }

    /** @deprecated without a replacement and no longer used. */
    @Deprecated
    public ClientRSocketFactory singleSubscriberRequester() {
      return this;
    }

    /**
     * Enables a reconnectable, shared instance of {@code Mono<RSocket>} so every subscriber will
     * observe the same RSocket instance up on connection establishment. <br>
     * For example:
     *
     * <pre>{@code
     * Mono<RSocket> sharedRSocketMono =
     *   RSocketFactory
     *                .connect()
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
    public ClientRSocketFactory reconnect(Retry retrySpec) {
      connector.reconnect(retrySpec);
      return this;
    }

    public ClientRSocketFactory resume() {
      resume = resume != null ? resume : new Resume();
      connector.resume(resume);
      return this;
    }

    public ClientRSocketFactory resumeToken(Supplier<ByteBuf> supplier) {
      resume();
      resume.token(supplier);
      return this;
    }

    public ClientRSocketFactory resumeStore(
        Function<? super ByteBuf, ? extends ResumableFramesStore> storeFactory) {
      resume();
      resume.storeFactory(storeFactory);
      return this;
    }

    public ClientRSocketFactory resumeSessionDuration(Duration sessionDuration) {
      resume();
      resume.sessionDuration(sessionDuration);
      return this;
    }

    public ClientRSocketFactory resumeStreamTimeout(Duration streamTimeout) {
      resume();
      resume.streamTimeout(streamTimeout);
      return this;
    }

    public ClientRSocketFactory resumeStrategy(Supplier<ResumeStrategy> strategy) {
      resume();
      resume.retry(
          Retry.from(
              signals -> signals.flatMap(s -> strategy.get().apply(CLIENT_RESUME, s.failure()))));
      return this;
    }

    public ClientRSocketFactory resumeCleanupOnKeepAlive() {
      resume();
      resume.cleanupStoreOnKeepAlive();
      return this;
    }

    public Start<RSocket> transport(Supplier<ClientTransport> transport) {
      return () -> connector.connect(transport);
    }

    public ClientTransportAcceptor acceptor(Function<RSocket, RSocket> acceptor) {
      return acceptor(() -> acceptor);
    }

    public ClientTransportAcceptor acceptor(Supplier<Function<RSocket, RSocket>> acceptorSupplier) {
      return acceptor(
          (setup, sendingSocket) -> {
            acceptorSupplier.get().apply(sendingSocket);
            return Mono.empty();
          });
    }

    public ClientTransportAcceptor acceptor(SocketAcceptor acceptor) {
      connector.acceptor(acceptor);
      return this;
    }

    public ClientRSocketFactory fragment(int mtu) {
      connector.fragment(mtu);
      return this;
    }

    /**
     * @deprecated this handler is deliberately no-ops and is deprecated with no replacement. In
     *     order to observe errors, it is recommended to add error handler using {@code doOnError}
     *     on the specific logical stream. In order to observe connection, or RSocket terminal
     *     errors, it is recommended to hook on {@link Closeable#onClose()} handler.
     */
    public ClientRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
      return this;
    }

    public ClientRSocketFactory setupPayload(Payload payload) {
      connector.setupPayload(payload);
      return this;
    }

    public ClientRSocketFactory frameDecoder(PayloadDecoder payloadDecoder) {
      connector.payloadDecoder(payloadDecoder);
      return this;
    }
  }

  /** Factory to create, configure, and start an RSocket server. */
  public static class ServerRSocketFactory implements ServerTransportAcceptor {
    private final RSocketServer server;

    private Resume resume;

    public ServerRSocketFactory() {
      this(RSocketServer.create());
    }

    public ServerRSocketFactory(RSocketServer server) {
      this.server = server;
    }

    /**
     * @deprecated this method is deprecated and deliberately has no effect anymore. Right now, in
     *     order configure the custom {@link ByteBufAllocator} it is recommended to use the
     *     following setup for Reactor Netty based transport: <br>
     *     1. For Client: <br>
     *     <pre>{@code
     * TcpClient.create()
     *          ...
     *          .bootstrap(bootstrap -> bootstrap.option(ChannelOption.ALLOCATOR, clientAllocator))
     * }</pre>
     *     <br>
     *     2. For server: <br>
     *     <pre>{@code
     * TcpServer.create()
     *          ...
     *          .bootstrap(serverBootstrap -> serverBootstrap.childOption(ChannelOption.ALLOCATOR, serverAllocator))
     * }</pre>
     *     Or in case of local transport, to use corresponding factory method {@code
     *     LocalClientTransport.creat(String, ByteBufAllocator)}
     * @param allocator instance of {@link ByteBufAllocator}
     * @return this factory instance
     */
    @Deprecated
    public ServerRSocketFactory byteBufAllocator(ByteBufAllocator allocator) {
      return this;
    }

    public ServerRSocketFactory addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
      server.interceptors(registry -> registry.forConnection(interceptor));
      return this;
    }
    /** Deprecated. Use {@link #addRequesterPlugin(RSocketInterceptor)} instead */
    @Deprecated
    public ServerRSocketFactory addClientPlugin(RSocketInterceptor interceptor) {
      return addRequesterPlugin(interceptor);
    }

    public ServerRSocketFactory addRequesterPlugin(RSocketInterceptor interceptor) {
      server.interceptors(registry -> registry.forRequester(interceptor));
      return this;
    }

    /** Deprecated. Use {@link #addResponderPlugin(RSocketInterceptor)} instead */
    @Deprecated
    public ServerRSocketFactory addServerPlugin(RSocketInterceptor interceptor) {
      return addResponderPlugin(interceptor);
    }

    public ServerRSocketFactory addResponderPlugin(RSocketInterceptor interceptor) {
      server.interceptors(registry -> registry.forResponder(interceptor));
      return this;
    }

    public ServerRSocketFactory addSocketAcceptorPlugin(SocketAcceptorInterceptor interceptor) {
      server.interceptors(registry -> registry.forSocketAcceptor(interceptor));
      return this;
    }

    public ServerTransportAcceptor acceptor(SocketAcceptor acceptor) {
      server.acceptor(acceptor);
      return this;
    }

    public ServerRSocketFactory frameDecoder(PayloadDecoder payloadDecoder) {
      server.payloadDecoder(payloadDecoder);
      return this;
    }

    public ServerRSocketFactory fragment(int mtu) {
      server.fragment(mtu);
      return this;
    }

    /**
     * @deprecated this handler is deliberately no-ops and is deprecated with no replacement. In
     *     order to observe errors, it is recommended to add error handler using {@code doOnError}
     *     on the specific logical stream. In order to observe connection, or RSocket terminal
     *     errors, it is recommended to hook on {@link Closeable#onClose()} handler.
     */
    public ServerRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
      return this;
    }

    public ServerRSocketFactory lease(Supplier<Leases<?>> supplier) {
      server.lease(supplier);
      return this;
    }

    public ServerRSocketFactory lease() {
      server.lease(Leases::new);
      return this;
    }

    /** @deprecated without a replacement and no longer used. */
    @Deprecated
    public ServerRSocketFactory singleSubscriberRequester() {
      return this;
    }

    public ServerRSocketFactory resume() {
      resume = resume != null ? resume : new Resume();
      server.resume(resume);
      return this;
    }

    public ServerRSocketFactory resumeStore(
        Function<? super ByteBuf, ? extends ResumableFramesStore> storeFactory) {
      resume();
      resume.storeFactory(storeFactory);
      return this;
    }

    public ServerRSocketFactory resumeSessionDuration(Duration sessionDuration) {
      resume();
      resume.sessionDuration(sessionDuration);
      return this;
    }

    public ServerRSocketFactory resumeStreamTimeout(Duration streamTimeout) {
      resume();
      resume.streamTimeout(streamTimeout);
      return this;
    }

    public ServerRSocketFactory resumeCleanupOnKeepAlive() {
      resume();
      resume.cleanupStoreOnKeepAlive();
      return this;
    }

    @Override
    public ServerTransport.ConnectionAcceptor toConnectionAcceptor() {
      return server.asConnectionAcceptor();
    }

    @Override
    public <T extends Closeable> Start<T> transport(Supplier<ServerTransport<T>> transport) {
      return () -> server.bind(transport.get());
    }
  }
}
