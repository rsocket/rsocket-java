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

package io.reactivesocket.local;

import io.netty.util.internal.ThreadLocalRandom;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.local.client.LocalReactiveSocketConnector;
import io.reactivesocket.local.server.LocalReactiveSocketServer;
import io.reactivesocket.test.ClientSetupRule;
import io.reactivesocket.test.TestRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.logging.LogLevel.*;

public class LocalClientSetupRule extends ClientSetupRule {

    private static final Logger logger = LoggerFactory.getLogger(LocalClientSetupRule.class);

    public LocalClientSetupRule() {
        super(LocalReactiveSocketConnector.create(ConnectionSetupPayload.create("", ""), Throwable::printStackTrace)
                                          .configureClient(client -> client.enableWireLogging("test-client", DEBUG)),
              () -> {
                  String id = "test-" + ThreadLocalRandom.current().nextInt(100);
                  logger.info("Local socket id: " + id);

                  return LocalReactiveSocketServer.create(id)
                                                  .configureServer(server -> server.enableWireLogging(id, DEBUG))
                                                  .start(new ConnectionSetupHandler() {
                @Override
                public RequestHandler apply(ConnectionSetupPayload s, ReactiveSocket rs) {
                    return new TestRequestHandler();
                }
            }).getServerAddress();
        });
    }

}
