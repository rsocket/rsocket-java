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
package io.rsocket.core;

import static io.rsocket.core.FragmentationUtils.assertMtu;
import static io.rsocket.core.PayloadValidationUtils.assertValidateSetup;
import static io.rsocket.core.ReassemblyUtils.assertInboundPayloadSize;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketClient;
import io.rsocket.SocketAcceptor;
import io.rsocket.frame.SetupFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.lease.LeaseStats;
import io.rsocket.lease.Leases;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.lease.ResponderLeaseHandler;
import io.rsocket.plugins.InitializingInterceptorRegistry;
import io.rsocket.plugins.InterceptorRegistry;
import io.rsocket.resume.ClientRSocketSession;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

/**
 * The main class to use to establish a connection to an RSocket server.
 *
 * <p>For using TCP using default settings:
 *
 * <pre>{@code
 * import io.rsocket.transport.netty.client.TcpClientTransport;
 *
 * RSocketClient client =
 *         RSocketConnector.createRSocketClient(TcpClientTransport.create("localhost", 7000));
 * }</pre>
 *
 * <p>To customize connection settings before connecting:
 *
 * <pre>{@code
 * RSocketClient client =
 *         RSocketConnector.create()
 *                 .metadataMimeType("message/x.rsocket.composite-metadata.v0")
 *                 .dataMimeType("application/cbor")
 *                 .toRSocketClient(TcpClientTransport.create("localhost", 7000));
 * }</pre>
 */
public class RSocketConnector {
  private static final String CLIENT_TAG = "client";

  private static final BiConsumer<RSocket, Invalidatable> INVALIDATE_FUNCTION =
      (r, i) -> r.onClose().subscribe(null, __ -> i.invalidate(), i::invalidate);

  private Mono<Payload> setupPayloadMono = Mono.empty();
  private String metadataMimeType = "application/binary";
  private String dataMimeType = "application/binary";
  private Duration keepAliveInterval = Duration.ofSeconds(20);
  private Duration keepAliveMaxLifeTime = Duration.ofSeconds(90);

  @Nullable private SocketAcceptor acceptor;
  private InitializingInterceptorRegistry interceptors = new InitializingInterceptorRegistry();

  private Retry retrySpec;
  private Resume resume;
  private Supplier<Leases<?>> leasesSupplier;

  private int mtu = 0;
  private int maxInboundPayloadSize = Integer.MAX_VALUE;
  private PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;

  private RSocketConnector() {}

  /**
   * Static factory method to create an {@code RSocketConnector} instance and customize default
   * settings before connecting. To connect only, use {@link #connectWith(ClientTransport)}.
   */
  public static RSocketConnector create() {
    return new RSocketConnector();
  }

  /**
   * Static factory method to connect with default settings, effectively a shortcut for:
   *
   * <pre class="code">
   * RSocketConnector.create().connectWith(transport);
   * </pre>
   *
   * @param transport the transport of choice to connect with
   * @return a {@code Mono} with the connected RSocket
   */
  public static Mono<RSocket> connectWith(ClientTransport transport) {
    return RSocketConnector.create().connect(() -> transport);
  }

  /**
   * @param transport
   * @return
   * @since 1.0.1
   */
  public static RSocketClient createRSocketClient(ClientTransport transport) {
    return RSocketConnector.create().toRSocketClient(transport);
  }

  /**
   * Provide a {@code Mono} from which to obtain the {@code Payload} for the initial SETUP frame.
   * Data and metadata should be formatted according to the MIME types specified via {@link
   * #dataMimeType(String)} and {@link #metadataMimeType(String)}.
   *
   * @param setupPayloadMono the payload with data and/or metadata for the {@code SETUP} frame.
   * @return the same instance for method chaining
   * @since 1.0.2
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-setup">SETUP
   *     Frame</a>
   */
  public RSocketConnector setupPayload(Mono<Payload> setupPayloadMono) {
    this.setupPayloadMono = setupPayloadMono;
    return this;
  }

  /**
   * Variant of {@link #setupPayload(Mono)} that accepts a {@code Payload} instance.
   *
   * <p>Note: if the given payload is {@link io.rsocket.util.ByteBufPayload}, it is copied to a
   * {@link DefaultPayload} and released immediately. This ensures it can re-used to obtain a
   * connection more than once.
   *
   * @param payload the payload with data and/or metadata for the {@code SETUP} frame.
   * @return the same instance for method chaining
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-setup">SETUP
   *     Frame</a>
   */
  public RSocketConnector setupPayload(Payload payload) {
    if (payload instanceof DefaultPayload) {
      this.setupPayloadMono = Mono.just(payload);
    } else {
      this.setupPayloadMono = Mono.just(DefaultPayload.create(Objects.requireNonNull(payload)));
      payload.release();
    }
    return this;
  }

  /**
   * Set the MIME type to use for formatting payload data on the established connection. This is set
   * in the initial {@code SETUP} frame sent to the server.
   *
   * <p>By default this is set to {@code "application/binary"}.
   *
   * @param dataMimeType the MIME type to be used for payload data
   * @return the same instance for method chaining
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-setup">SETUP
   *     Frame</a>
   */
  public RSocketConnector dataMimeType(String dataMimeType) {
    this.dataMimeType = Objects.requireNonNull(dataMimeType);
    return this;
  }

  /**
   * Set the MIME type to use for formatting payload metadata on the established connection. This is
   * set in the initial {@code SETUP} frame sent to the server.
   *
   * <p>For metadata encoding, consider using one of the following encoders:
   *
   * <ul>
   *   <li>{@link io.rsocket.metadata.CompositeMetadataFlyweight Composite Metadata}
   *   <li>{@link io.rsocket.metadata.TaggingMetadataFlyweight Routing}
   *   <li>{@link io.rsocket.metadata.security.AuthMetadataFlyweight Authentication}
   * </ul>
   *
   * <p>For more on the above metadata formats, see the corresponding <a
   * href="https://github.com/rsocket/rsocket/tree/master/Extensions">protocol extensions</a>
   *
   * <p>By default this is set to {@code "application/binary"}.
   *
   * @param metadataMimeType the MIME type to be used for payload metadata
   * @return the same instance for method chaining
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-setup">SETUP
   *     Frame</a>
   */
  public RSocketConnector metadataMimeType(String metadataMimeType) {
    this.metadataMimeType = Objects.requireNonNull(metadataMimeType);
    return this;
  }

  /**
   * Set the "Time Between {@code KEEPALIVE} Frames" which is how frequently {@code KEEPALIVE}
   * frames should be emitted, and the "Max Lifetime" which is how long to allow between {@code
   * KEEPALIVE} frames from the remote end before concluding that connectivity is lost. Both
   * settings are specified in the initial {@code SETUP} frame sent to the server. The spec mentions
   * the following:
   *
   * <ul>
   *   <li>For server-to-server connections, a reasonable time interval between client {@code
   *       KEEPALIVE} frames is 500ms.
   *   <li>For mobile-to-server connections, the time interval between client {@code KEEPALIVE}
   *       frames is often > 30,000ms.
   * </ul>
   *
   * <p>By default these are set to 20 seconds and 90 seconds respectively.
   *
   * @param interval how frequently to emit KEEPALIVE frames
   * @param maxLifeTime how long to allow between {@code KEEPALIVE} frames from the remote end
   *     before assuming that connectivity is lost; the value should be generous and allow for
   *     multiple missed {@code KEEPALIVE} frames.
   * @return the same instance for method chaining
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-setup">SETUP
   *     Frame</a>
   */
  public RSocketConnector keepAlive(Duration interval, Duration maxLifeTime) {
    if (!interval.negated().isNegative()) {
      throw new IllegalArgumentException("`interval` for keepAlive must be > 0");
    }
    if (!maxLifeTime.negated().isNegative()) {
      throw new IllegalArgumentException("`maxLifeTime` for keepAlive must be > 0");
    }
    this.keepAliveInterval = interval;
    this.keepAliveMaxLifeTime = maxLifeTime;
    return this;
  }

  /**
   * Configure interception at one of the following levels:
   *
   * <ul>
   *   <li>Transport level
   *   <li>At the level of accepting new connections
   *   <li>Performing requests
   *   <li>Responding to requests
   * </ul>
   *
   * @param configurer a configurer to customize interception with.
   * @return the same instance for method chaining
   * @see io.rsocket.plugins.LimitRateInterceptor
   */
  public RSocketConnector interceptors(Consumer<InterceptorRegistry> configurer) {
    configurer.accept(this.interceptors);
    return this;
  }

  /**
   * Configure a client-side {@link SocketAcceptor} for responding to requests from the server.
   *
   * <p>A full-form example with access to the {@code SETUP} frame and the "sending" RSocket (the
   * same as the one returned from {@link #connect(ClientTransport)}):
   *
   * <pre>{@code
   * Mono<RSocket> rsocketMono =
   *     RSocketConnector.create()
   *             .acceptor((setup, sendingRSocket) -> Mono.just(new RSocket() {...}))
   *             .connect(transport);
   * }</pre>
   *
   * <p>A shortcut example with just the handling RSocket:
   *
   * <pre>{@code
   * Mono<RSocket> rsocketMono =
   *     RSocketConnector.create()
   *             .acceptor(SocketAcceptor.with(new RSocket() {...})))
   *             .connect(transport);
   * }</pre>
   *
   * <p>A shortcut example handling only request-response:
   *
   * <pre>{@code
   * Mono<RSocket> rsocketMono =
   *     RSocketConnector.create()
   *             .acceptor(SocketAcceptor.forRequestResponse(payload -> ...))
   *             .connect(transport);
   * }</pre>
   *
   * <p>By default, {@code new RSocket(){}} is used which rejects all requests from the server with
   * {@link UnsupportedOperationException}.
   *
   * @param acceptor the acceptor to use for responding to server requests
   * @return the same instance for method chaining
   */
  public RSocketConnector acceptor(SocketAcceptor acceptor) {
    this.acceptor = acceptor;
    return this;
  }

  /**
   * When this is enabled, the connect methods of this class return a special {@code Mono<RSocket>}
   * that maintains a single, shared {@code RSocket} for all subscribers:
   *
   * <pre>{@code
   * Mono<RSocket> rsocketMono =
   *   RSocketConnector.create()
   *           .reconnect(Retry.fixedDelay(3, Duration.ofSeconds(1)))
   *           .connect(transport);
   *
   *  RSocket r1 = rsocketMono.block();
   *  RSocket r2 = rsocketMono.block();
   *
   *  assert r1 == r2;
   * }</pre>
   *
   * <p>The {@code RSocket} remains cached until the connection is lost and after that, new attempts
   * to subscribe or re-subscribe trigger a reconnect and result in a new shared {@code RSocket}:
   *
   * <pre>{@code
   * Mono<RSocket> rsocketMono =
   *   RSocketConnector.create()
   *           .reconnect(Retry.fixedDelay(3, Duration.ofSeconds(1)))
   *           .connect(transport);
   *
   *  RSocket r1 = rsocketMono.block();
   *  RSocket r2 = rsocketMono.block();
   *
   *  r1.dispose();
   *
   *  RSocket r3 = rsocketMono.block();
   *  RSocket r4 = rsocketMono.block();
   *
   *  assert r1 == r2;
   *  assert r3 == r4;
   *  assert r1 != r3;
   *
   * }</pre>
   *
   * <p>Downstream subscribers for individual requests still need their own retry logic to determine
   * if or when failed requests should be retried which in turn triggers the shared reconnect:
   *
   * <pre>{@code
   * Mono<RSocket> rocketMono =
   *   RSocketConnector.create()
   *           .reconnect(Retry.fixedDelay(3, Duration.ofSeconds(1)))
   *           .connect(transport);
   *
   *  rsocketMono.flatMap(rsocket -> rsocket.requestResponse(...))
   *           .retryWhen(Retry.fixedDelay(1, Duration.ofSeconds(5)))
   *           .subscribe()
   * }</pre>
   *
   * <p><strong>Note:</strong> this feature is mutually exclusive with {@link #resume(Resume)}. If
   * both are enabled, "resume" takes precedence. Consider using "reconnect" when the server does
   * not have "resume" enabled or supported, or when you don't need to incur the overhead of saving
   * in-flight frames to be potentially replayed after a reconnect.
   *
   * <p>By default this is not enabled in which case a new connection is obtained per subscriber.
   *
   * @param retry a retry spec that declares the rules for reconnecting
   * @return the same instance for method chaining
   */
  public RSocketConnector reconnect(Retry retry) {
    this.retrySpec = Objects.requireNonNull(retry);
    return this;
  }

  /**
   * Enables the Resume capability of the RSocket protocol where if the client gets disconnected,
   * the connection is re-acquired and any interrupted streams are resumed automatically. For this
   * to work the server must also support and have the Resume capability enabled.
   *
   * <p>See {@link Resume} for settings to customize the Resume capability.
   *
   * <p><strong>Note:</strong> this feature is mutually exclusive with {@link #reconnect(Retry)}. If
   * both are enabled, "resume" takes precedence. Consider using "reconnect" when the server does
   * not have "resume" enabled or supported, or when you don't need to incur the overhead of saving
   * in-flight frames to be potentially replayed after a reconnect.
   *
   * <p>By default this is not enabled.
   *
   * @param resume configuration for the Resume capability
   * @return the same instance for method chaining
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#resuming-operation">Resuming
   *     Operation</a>
   */
  public RSocketConnector resume(Resume resume) {
    this.resume = resume;
    return this;
  }

  /**
   * Enables the Lease feature of the RSocket protocol where the number of requests that can be
   * performed from either side are rationed via {@code LEASE} frames from the responder side.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * Mono<RSocket> rocketMono =
   *         RSocketConnector.create().lease(Leases::new).connect(transport);
   * }</pre>
   *
   * <p>By default this is not enabled.
   *
   * @param supplier supplier for a {@link Leases}
   * @return the same instance for method chaining
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#lease-semantics">Lease
   *     Semantics</a>
   */
  public RSocketConnector lease(Supplier<Leases<? extends LeaseStats>> supplier) {
    this.leasesSupplier = supplier;
    return this;
  }

  /**
   * When this is set, frames reassembler control maximum payload size which can be reassembled.
   *
   * <p>By default this is not set in which case maximum reassembled payloads size is not
   * controlled.
   *
   * @param maxInboundPayloadSize the threshold size for reassembly, must no be less than 64 bytes.
   *     Please note, {@code maxInboundPayloadSize} must always be greater or equal to {@link
   *     io.rsocket.transport.Transport#maxFrameLength()}, otherwise inbound frame can exceed the
   *     {@code maxInboundPayloadSize}
   * @return the same instance for method chaining
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#fragmentation-and-reassembly">Fragmentation
   *     and Reassembly</a>
   */
  public RSocketConnector maxInboundPayloadSize(int maxInboundPayloadSize) {
    this.maxInboundPayloadSize = assertInboundPayloadSize(maxInboundPayloadSize);
    return this;
  }

  /**
   * When this is set, frames larger than the given maximum transmission unit (mtu) size value are
   * broken down into fragments to fit that size.
   *
   * <p>By default this is not set in which case payloads are sent whole up to the maximum frame
   * size of 16,777,215 bytes.
   *
   * @param mtu the threshold size for fragmentation, must be no less than 64
   * @return the same instance for method chaining
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#fragmentation-and-reassembly">Fragmentation
   *     and Reassembly</a>
   */
  public RSocketConnector fragment(int mtu) {
    this.mtu = assertMtu(mtu);
    return this;
  }

  /**
   * Configure the {@code PayloadDecoder} used to create {@link Payload}'s from incoming raw frame
   * buffers. The following decoders are available:
   *
   * <ul>
   *   <li>{@link PayloadDecoder#DEFAULT} -- the data and metadata are independent copies of the
   *       underlying frame {@link ByteBuf}
   *   <li>{@link PayloadDecoder#ZERO_COPY} -- the data and metadata are retained slices of the
   *       underlying {@link ByteBuf}. That's more efficient but requires careful tracking and
   *       {@link Payload#release() release} of the payload when no longer needed.
   * </ul>
   *
   * <p>By default this is set to {@link PayloadDecoder#DEFAULT} in which case data and metadata are
   * copied and do not need to be tracked and released.
   *
   * @param decoder the decoder to use
   * @return the same instance for method chaining
   */
  public RSocketConnector payloadDecoder(PayloadDecoder decoder) {
    Objects.requireNonNull(decoder);
    this.payloadDecoder = decoder;
    return this;
  }

  /**
   * Create {@link RSocketClient} that will use {@link #connect(ClientTransport)} as its source to
   * obtain a live, shared {@code RSocket} when the first request is made, and also on subsequent
   * requests after the connection is lost.
   *
   * <p>The following transports are available through additional RSocket Java modules:
   *
   * <ul>
   *   <li>{@link io.rsocket.transport.netty.client.TcpClientTransport TcpClientTransport} via
   *       {@code rsocket-transport-netty}.
   *   <li>{@link io.rsocket.transport.netty.client.WebsocketClientTransport
   *       WebsocketClientTransport} via {@code rsocket-transport-netty}.
   *   <li>{@link io.rsocket.transport.local.LocalClientTransport LocalClientTransport} via {@code
   *       rsocket-transport-local}
   * </ul>
   *
   * @param transport the transport of choice to connect with
   * @return a {@code RSocketClient} with not established connection. Note, connection will be
   *     established on the first request
   * @since 1.0.1
   */
  public RSocketClient toRSocketClient(ClientTransport transport) {
    Mono<RSocket> source = connect0(() -> transport);

    if (retrySpec != null) {
      source = source.retryWhen(retrySpec);
    }

    return new DefaultRSocketClient(source);
  }

  /**
   * Connect with the given transport and obtain a live {@link RSocket} to use for making requests.
   * Each subscriber to the returned {@code Mono} receives a new connection, if neither {@link
   * #reconnect(Retry) reconnect} nor {@link #resume(Resume)} are enabled.
   *
   * <p>The following transports are available through additional RSocket Java modules:
   *
   * <ul>
   *   <li>{@link io.rsocket.transport.netty.client.TcpClientTransport TcpClientTransport} via
   *       {@code rsocket-transport-netty}.
   *   <li>{@link io.rsocket.transport.netty.client.WebsocketClientTransport
   *       WebsocketClientTransport} via {@code rsocket-transport-netty}.
   *   <li>{@link io.rsocket.transport.local.LocalClientTransport LocalClientTransport} via {@code
   *       rsocket-transport-local}
   * </ul>
   *
   * @param transport the transport of choice to connect with
   * @return a {@code Mono} with the connected RSocket
   */
  public Mono<RSocket> connect(ClientTransport transport) {
    return connect(() -> transport);
  }

  /**
   * Variant of {@link #connect(ClientTransport)} with a {@link Supplier} for the {@code
   * ClientTransport}.
   *
   * <p>// TODO: when to use?
   *
   * @param transportSupplier supplier for the transport to connect with
   * @return a {@code Mono} with the connected RSocket
   */
  public Mono<RSocket> connect(Supplier<ClientTransport> transportSupplier) {
    return this.connect0(transportSupplier)
        .as(
            source -> {
              if (retrySpec != null) {
                return new ReconnectMono<>(
                    source.retryWhen(retrySpec), Disposable::dispose, INVALIDATE_FUNCTION);
              } else {
                return source;
              }
            });
  }

  private Mono<RSocket> connect0(Supplier<ClientTransport> transportSupplier) {
    return Mono.fromSupplier(transportSupplier)
        .flatMap(
            ct -> {
              int maxFrameLength = ct.maxFrameLength();

              Mono<DuplexConnection> connectionMono =
                  Mono.fromCallable(
                          () -> {
                            assertValidateSetup(maxFrameLength, maxInboundPayloadSize, mtu);
                            return ct;
                          })
                      .flatMap(transport -> transport.connect());

              return connectionMono
                  .flatMap(
                      connection ->
                          setupPayloadMono
                              .defaultIfEmpty(EmptyPayload.INSTANCE)
                              .map(setupPayload -> Tuples.of(connection, setupPayload))
                              .doOnError(ex -> connection.dispose())
                              .doOnCancel(connection::dispose))
                  .flatMap(
                      tuple -> {
                        DuplexConnection connection = tuple.getT1();
                        Payload setupPayload = tuple.getT2();
                        ByteBuf resumeToken;
                        KeepAliveHandler keepAliveHandler;
                        DuplexConnection wrappedConnection;

                        if (resume != null) {
                          resumeToken = resume.getTokenSupplier().get();
                          ClientRSocketSession session =
                              new ClientRSocketSession(
                                      connection,
                                      resume.getSessionDuration(),
                                      resume.getRetry(),
                                      resume.getStoreFactory(CLIENT_TAG).apply(resumeToken),
                                      resume.getStreamTimeout(),
                                      resume.isCleanupStoreOnKeepAlive())
                                  .continueWith(connectionMono)
                                  .resumeToken(resumeToken);
                          keepAliveHandler =
                              new KeepAliveHandler.ResumableKeepAliveHandler(
                                  session.resumableConnection());
                          wrappedConnection = session.resumableConnection();
                        } else {
                          resumeToken = Unpooled.EMPTY_BUFFER;
                          keepAliveHandler =
                              new KeepAliveHandler.DefaultKeepAliveHandler(connection);
                          wrappedConnection = connection;
                        }

                        ClientServerInputMultiplexer multiplexer =
                            new ClientServerInputMultiplexer(wrappedConnection, interceptors, true);

                        boolean leaseEnabled = leasesSupplier != null;
                        Leases<?> leases = leaseEnabled ? leasesSupplier.get() : null;
                        RequesterLeaseHandler requesterLeaseHandler =
                            leaseEnabled
                                ? new RequesterLeaseHandler.Impl(CLIENT_TAG, leases.receiver())
                                : RequesterLeaseHandler.None;

                        RSocket rSocketRequester =
                            new RSocketRequester(
                                multiplexer.asClientConnection(),
                                payloadDecoder,
                                StreamIdSupplier.clientSupplier(),
                                mtu,
                                maxFrameLength,
                                maxInboundPayloadSize,
                                (int) keepAliveInterval.toMillis(),
                                (int) keepAliveMaxLifeTime.toMillis(),
                                keepAliveHandler,
                                requesterLeaseHandler);

                        RSocket wrappedRSocketRequester =
                            interceptors.initRequester(rSocketRequester);

                        ByteBuf setupFrame =
                            SetupFrameCodec.encode(
                                wrappedConnection.alloc(),
                                leaseEnabled,
                                (int) keepAliveInterval.toMillis(),
                                (int) keepAliveMaxLifeTime.toMillis(),
                                resumeToken,
                                metadataMimeType,
                                dataMimeType,
                                setupPayload);

                        SocketAcceptor acceptor =
                            this.acceptor != null
                                ? this.acceptor
                                : SocketAcceptor.with(new RSocket() {});

                        ConnectionSetupPayload setup =
                            new DefaultConnectionSetupPayload(setupFrame);

                        return interceptors
                            .initSocketAcceptor(acceptor)
                            .accept(setup, wrappedRSocketRequester)
                            .flatMap(
                                rSocketHandler -> {
                                  RSocket wrappedRSocketHandler =
                                      interceptors.initResponder(rSocketHandler);

                                  ResponderLeaseHandler responderLeaseHandler =
                                      leaseEnabled
                                          ? new ResponderLeaseHandler.Impl<>(
                                              CLIENT_TAG,
                                              wrappedConnection.alloc(),
                                              leases.sender(),
                                              leases.stats())
                                          : ResponderLeaseHandler.None;

                                  RSocket rSocketResponder =
                                      new RSocketResponder(
                                          multiplexer.asServerConnection(),
                                          wrappedRSocketHandler,
                                          payloadDecoder,
                                          responderLeaseHandler,
                                          mtu,
                                          maxFrameLength,
                                          maxInboundPayloadSize);

                                  return wrappedConnection
                                      .sendOne(setupFrame.retain())
                                      .thenReturn(wrappedRSocketRequester);
                                })
                            .doFinally(signalType -> setup.release());
                      });
            });
  }
}
