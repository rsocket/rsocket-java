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
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketErrorException;
import io.rsocket.SocketAcceptor;
import io.rsocket.exceptions.InvalidSetupException;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.SetupFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.lease.Leases;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.lease.ResponderLeaseHandler;
import io.rsocket.plugins.InitializingInterceptorRegistry;
import io.rsocket.plugins.InterceptorRegistry;
import io.rsocket.resume.SessionManager;
import io.rsocket.transport.ServerTransport;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/**
 * The main class for starting an RSocket server.
 *
 * <p>For example:
 *
 * <pre>{@code
 * CloseableChannel closeable =
 *         RSocketServer.create(SocketAcceptor.with(new RSocket() {...}))
 *                 .bind(TcpServerTransport.create("localhost", 7000))
 *                 .block();
 * }</pre>
 */
public final class RSocketServer {
  private static final String SERVER_TAG = "server";

  private SocketAcceptor acceptor = SocketAcceptor.with(new RSocket() {});
  private InitializingInterceptorRegistry interceptors = new InitializingInterceptorRegistry();

  private Resume resume;
  private Supplier<Leases<?>> leasesSupplier = null;

  private int mtu = 0;
  private int maxInboundPayloadSize = Integer.MAX_VALUE;
  private PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;

  private RSocketServer() {}

  /** Static factory method to create an {@code RSocketServer}. */
  public static RSocketServer create() {
    return new RSocketServer();
  }

  /**
   * Static factory method to create an {@code RSocketServer} instance with the given {@code
   * SocketAcceptor}. Effectively a shortcut for:
   *
   * <pre class="code">
   * RSocketServer.create().acceptor(...);
   * </pre>
   *
   * @param acceptor the acceptor to handle connections with
   * @return the same instance for method chaining
   * @see #acceptor(SocketAcceptor)
   */
  public static RSocketServer create(SocketAcceptor acceptor) {
    return RSocketServer.create().acceptor(acceptor);
  }

  /**
   * Set the acceptor to handle incoming connections and handle requests.
   *
   * <p>An example with access to the {@code SETUP} frame and sending RSocket for performing
   * requests back to the client if needed:
   *
   * <pre>{@code
   * RSocketServer.create((setup, sendingRSocket) -> Mono.just(new RSocket() {...}))
   *         .bind(TcpServerTransport.create("localhost", 7000))
   *         .subscribe();
   * }</pre>
   *
   * <p>A shortcut to provide the handling RSocket only:
   *
   * <pre>{@code
   * RSocketServer.create(SocketAcceptor.with(new RSocket() {...}))
   *         .bind(TcpServerTransport.create("localhost", 7000))
   *         .subscribe();
   * }</pre>
   *
   * <p>A shortcut to handle request-response interactions only:
   *
   * <pre>{@code
   * RSocketServer.create(SocketAcceptor.forRequestResponse(payload -> ...))
   *         .bind(TcpServerTransport.create("localhost", 7000))
   *         .subscribe();
   * }</pre>
   *
   * <p>By default, {@code new RSocket(){}} is used for handling which rejects requests from the
   * client with {@link UnsupportedOperationException}.
   *
   * @param acceptor the acceptor to handle incoming connections and requests with
   * @return the same instance for method chaining
   */
  public RSocketServer acceptor(SocketAcceptor acceptor) {
    Objects.requireNonNull(acceptor);
    this.acceptor = acceptor;
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
  public RSocketServer interceptors(Consumer<InterceptorRegistry> configurer) {
    configurer.accept(this.interceptors);
    return this;
  }

  /**
   * Enables the Resume capability of the RSocket protocol where if the client gets disconnected,
   * the connection is re-acquired and any interrupted streams are transparently resumed. For this
   * to work clients must also support and request to enable this when connecting.
   *
   * <p>Use the {@link Resume} argument to customize the Resume session duration, storage, retry
   * logic, and others.
   *
   * <p>By default this is not enabled.
   *
   * @param resume configuration for the Resume capability
   * @return the same instance for method chaining
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#resuming-operation">Resuming
   *     Operation</a>
   */
  public RSocketServer resume(Resume resume) {
    this.resume = resume;
    return this;
  }

  /**
   * Enables the Lease feature of the RSocket protocol where the number of requests that can be
   * performed from either side are rationed via {@code LEASE} frames from the responder side. For
   * this to work clients must also support and request to enable this when connecting.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * RSocketServer.create(SocketAcceptor.with(new RSocket() {...}))
   *         .lease(Leases::new)
   *         .bind(TcpServerTransport.create("localhost", 7000))
   *         .subscribe();
   * }</pre>
   *
   * <p>By default this is not enabled.
   *
   * @param supplier supplier for a {@link Leases}
   * @return the same instance for method chaining
   * @return the same instance for method chaining
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#lease-semantics">Lease
   *     Semantics</a>
   */
  public RSocketServer lease(Supplier<Leases<?>> supplier) {
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
  public RSocketServer maxInboundPayloadSize(int maxInboundPayloadSize) {
    this.maxInboundPayloadSize = assertInboundPayloadSize(maxInboundPayloadSize);
    return this;
  }

  /**
   * When this is set, frames larger than the given maximum transmission unit (mtu) size value are
   * fragmented.
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
  public RSocketServer fragment(int mtu) {
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
  public RSocketServer payloadDecoder(PayloadDecoder decoder) {
    Objects.requireNonNull(decoder);
    this.payloadDecoder = decoder;
    return this;
  }

  /**
   * Start the server on the given transport.
   *
   * <p>The following transports are available from additional RSocket Java modules:
   *
   * <ul>
   *   <li>{@link io.rsocket.transport.netty.client.TcpServerTransport TcpServerTransport} via
   *       {@code rsocket-transport-netty}.
   *   <li>{@link io.rsocket.transport.netty.client.WebsocketServerTransport
   *       WebsocketServerTransport} via {@code rsocket-transport-netty}.
   *   <li>{@link io.rsocket.transport.local.LocalServerTransport LocalServerTransport} via {@code
   *       rsocket-transport-local}
   * </ul>
   *
   * @param transport the transport of choice to connect with
   * @param <T> the type of {@code Closeable} for the given transport
   * @return a {@code Mono} with a {@code Closeable} that can be used to obtain information about
   *     the server, stop it, or be notified of when it is stopped.
   */
  public <T extends Closeable> Mono<T> bind(ServerTransport<T> transport) {
    return Mono.defer(
        new Supplier<Mono<T>>() {
          final ServerSetup serverSetup = serverSetup();

          @Override
          public Mono<T> get() {
            int maxFrameLength = transport.maxFrameLength();
            assertValidateSetup(maxFrameLength, maxInboundPayloadSize, mtu);
            return transport
                .start(duplexConnection -> acceptor(serverSetup, duplexConnection, maxFrameLength))
                .doOnNext(c -> c.onClose().doFinally(v -> serverSetup.dispose()).subscribe());
          }
        });
  }

  /**
   * Start the server on the given transport. Effectively is a shortcut for {@code
   * .bind(ServerTransport).block()}
   */
  public <T extends Closeable> T bindNow(ServerTransport<T> transport) {
    return bind(transport).block();
  }
  /**
   * An alternative to {@link #bind(ServerTransport)} that is useful for installing RSocket on a
   * server that is started independently.
   *
   * @see io.rsocket.examples.transport.ws.WebSocketHeadersSample
   */
  public ServerTransport.ConnectionAcceptor asConnectionAcceptor() {
    return asConnectionAcceptor(FRAME_LENGTH_MASK);
  }

  /**
   * An alternative to {@link #bind(ServerTransport)} that is useful for installing RSocket on a
   * server that is started independently.
   *
   * @see io.rsocket.examples.transport.ws.WebSocketHeadersSample
   */
  public ServerTransport.ConnectionAcceptor asConnectionAcceptor(int maxFrameLength) {
    assertValidateSetup(maxFrameLength, maxInboundPayloadSize, mtu);
    return new ServerTransport.ConnectionAcceptor() {
      private final ServerSetup serverSetup = serverSetup();

      @Override
      public Mono<Void> apply(DuplexConnection connection) {
        return acceptor(serverSetup, connection, maxFrameLength);
      }
    };
  }

  private Mono<Void> acceptor(
      ServerSetup serverSetup, DuplexConnection connection, int maxFrameLength) {

    ClientServerInputMultiplexer multiplexer =
        new ClientServerInputMultiplexer(connection, interceptors, false);

    return multiplexer
        .asSetupConnection()
        .receive()
        .next()
        .flatMap(startFrame -> accept(serverSetup, startFrame, multiplexer, maxFrameLength));
  }

  private void acceptResume(
      ServerSetup serverSetup, ByteBuf resumeFrame, ClientServerInputMultiplexer multiplexer) {
    serverSetup.acceptRSocketResume(resumeFrame, multiplexer);
  }

  private Mono<Void> accept(
      ServerSetup serverSetup,
      ByteBuf startFrame,
      ClientServerInputMultiplexer multiplexer,
      int maxFrameLength) {
    switch (FrameHeaderCodec.frameType(startFrame)) {
      case SETUP:
        return acceptSetup(serverSetup, startFrame, multiplexer, maxFrameLength);
      case RESUME:
        acceptResume(serverSetup, startFrame, multiplexer);
        break;
      default:
        serverSetup.sendError(
            multiplexer,
            new InvalidSetupException(
                "invalid setup frame: " + FrameHeaderCodec.frameType(startFrame)));
    }

    return Mono.empty();
  }

  private Mono<Void> acceptSetup(
      ServerSetup serverSetup,
      ByteBuf setupFrame,
      ClientServerInputMultiplexer multiplexer,
      int maxFrameLength) {

    if (!SetupFrameCodec.isSupportedVersion(setupFrame)) {
      serverSetup.sendError(
          multiplexer,
          new InvalidSetupException(
              "Unsupported version: " + SetupFrameCodec.humanReadableVersion(setupFrame)));
    }

    boolean leaseEnabled = leasesSupplier != null;
    if (SetupFrameCodec.honorLease(setupFrame) && !leaseEnabled) {
      serverSetup.sendError(multiplexer, new InvalidSetupException("lease is not supported"));
      return Mono.empty();
    }

    return serverSetup.acceptRSocketSetup(
        setupFrame,
        multiplexer,
        (keepAliveHandler, wrappedMultiplexer) -> {
          ConnectionSetupPayload setupPayload =
              new DefaultConnectionSetupPayload(setupFrame.retain());

          Leases<?> leases = leaseEnabled ? leasesSupplier.get() : null;
          RequesterLeaseHandler requesterLeaseHandler =
              leaseEnabled
                  ? new RequesterLeaseHandler.Impl(SERVER_TAG, leases.receiver())
                  : RequesterLeaseHandler.None;

          RSocket rSocketRequester =
              new RSocketRequester(
                  wrappedMultiplexer.asServerConnection(),
                  payloadDecoder,
                  StreamIdSupplier.serverSupplier(),
                  mtu,
                  maxFrameLength,
                  maxInboundPayloadSize,
                  setupPayload.keepAliveInterval(),
                  setupPayload.keepAliveMaxLifetime(),
                  keepAliveHandler,
                  requesterLeaseHandler);

          RSocket wrappedRSocketRequester = interceptors.initRequester(rSocketRequester);

          return interceptors
              .initSocketAcceptor(acceptor)
              .accept(setupPayload, wrappedRSocketRequester)
              .doOnError(err -> serverSetup.sendError(multiplexer, rejectedSetupError(err)))
              .doOnNext(
                  rSocketHandler -> {
                    RSocket wrappedRSocketHandler = interceptors.initResponder(rSocketHandler);
                    DuplexConnection connection = wrappedMultiplexer.asClientConnection();

                    ResponderLeaseHandler responderLeaseHandler =
                        leaseEnabled
                            ? new ResponderLeaseHandler.Impl<>(
                                SERVER_TAG, connection.alloc(), leases.sender(), leases.stats())
                            : ResponderLeaseHandler.None;

                    RSocket rSocketResponder =
                        new RSocketResponder(
                            connection,
                            wrappedRSocketHandler,
                            payloadDecoder,
                            responderLeaseHandler,
                            mtu,
                            maxFrameLength,
                            maxInboundPayloadSize);
                  })
              .doFinally(signalType -> setupPayload.release())
              .then();
        });
  }

  private ServerSetup serverSetup() {
    return resume != null ? createSetup() : new ServerSetup.DefaultServerSetup();
  }

  ServerSetup createSetup() {
    return new ServerSetup.ResumableServerSetup(
        new SessionManager(),
        resume.getSessionDuration(),
        resume.getStreamTimeout(),
        resume.getStoreFactory(SERVER_TAG),
        resume.isCleanupStoreOnKeepAlive());
  }

  private RSocketErrorException rejectedSetupError(Throwable err) {
    String msg = err.getMessage();
    return new RejectedSetupException(msg == null ? "rejected by server acceptor" : msg);
  }
}
