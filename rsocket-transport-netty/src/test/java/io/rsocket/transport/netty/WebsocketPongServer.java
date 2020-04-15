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

package io.rsocket.transport.netty;

import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.test.PingHandler;
import io.rsocket.transport.netty.server.WebsocketServerTransport;

public final class WebsocketPongServer {

  public static void main(String... args) {
    RSocketServer.create(new PingHandler())
        .payloadDecoder(PayloadDecoder.ZERO_COPY)
        .bind(WebsocketServerTransport.create(7878))
        .block()
        .onClose()
        .block();
  }
}
