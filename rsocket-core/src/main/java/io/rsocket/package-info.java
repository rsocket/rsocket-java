/*
 * Copyright 2015-2020 the original author or authors.
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

/**
 * Contains key contracts of the RSocket programming model including {@link io.rsocket.RSocket
 * RSocket} for performing or handling RSocket interactions, {@link io.rsocket.SocketAcceptor
 * SocketAcceptor} for declaring responders, {@link io.rsocket.Payload Payload} for access to the
 * content of a payload, and others.
 *
 * <p>To connect to or start a server see {@link io.rsocket.core.RSocketConnector RSocketConnector}
 * and {@link io.rsocket.core.RSocketServer RSocketServer} in {@link io.rsocket.core}.
 */
@NonNullApi
package io.rsocket;

import reactor.util.annotation.NonNullApi;
