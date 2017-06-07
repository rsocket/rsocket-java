package io.rsocket;

import io.rsocket.exceptions.SetupException;
import reactor.core.publisher.Mono;

/**
 * {@code RSocket} is a full duplex protocol where a client and server are identical in terms of
 * both having the capability to initiate requests to their peer. This interface provides the
 * contract where a server accepts a new {@code RSocket} for sending requests to the peer and
 * returns a new {@code RSocket} that will be used to accept requests from it's peer.
 */
public interface SocketAcceptor {

  /**
   * Accepts a new {@code RSocket} used to send requests to the peer and returns another {@code
   * RSocket} that is used for accepting requests from the peer.
   *
   * @param setup Setup as sent by the client.
   * @param sendingSocket Socket used to send requests to the peer.
   * @return Socket to accept requests from the peer.
   * @throws SetupException If the acceptor needs to reject the setup of this socket.
   */
  Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket);
}
