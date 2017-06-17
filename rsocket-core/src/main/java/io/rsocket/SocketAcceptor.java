/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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
