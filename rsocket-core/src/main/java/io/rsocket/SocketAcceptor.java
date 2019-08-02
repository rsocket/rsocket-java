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

package io.rsocket;

import io.rsocket.exceptions.SetupException;
import reactor.core.publisher.Mono;

/**
 * RSocket is a full duplex protocol where a client and server are identical in terms of both having
 * the capability to initiate requests to their peer. This interface provides the contract where a
 * client or server handles the {@code setup} for a new connection and creates a responder {@code
 * RSocket} for accepting requests from the remote peer.
 */
public interface SocketAcceptor {

  /**
   * Handle the {@code SETUP} frame for a new connection and create a responder {@code RSocket} for
   * handling requests from the remote peer.
   *
   * @param setup the {@code setup} received from a client in a server scenario, or in a client
   *     scenario this is the setup about to be sent to the server.
   * @param sendingSocket socket for sending requests to the remote peer.
   * @return {@code RSocket} to accept requests with.
   * @throws SetupException If the acceptor needs to reject the setup of this socket.
   */
  Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket);
}
