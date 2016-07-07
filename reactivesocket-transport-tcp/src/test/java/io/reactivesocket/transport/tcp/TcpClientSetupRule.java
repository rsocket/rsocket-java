/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.transport.tcp;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.test.ClientSetupRule;
import io.reactivesocket.test.TestRequestHandler;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer;

import static io.netty.handler.logging.LogLevel.*;

public class TcpClientSetupRule extends ClientSetupRule {

    public TcpClientSetupRule() {
        super(TcpReactiveSocketConnector.create(ConnectionSetupPayload.create("", ""), Throwable::printStackTrace)
                                        .configureClient(client -> client.enableWireLogging("test-client",
                                                                                            DEBUG)),
        () -> {
            return TcpReactiveSocketServer.create(0)
                                          .configureServer(server -> server.enableWireLogging("test-server", DEBUG))
                                          .start(new ConnectionSetupHandler() {
                                              @Override
                                              public RequestHandler apply(ConnectionSetupPayload s, ReactiveSocket rs) {
                                                  return new TestRequestHandler();
                                              }
                                          }).getServerAddress();
        });
    }

}
