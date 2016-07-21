/*
 * Copyright 2016 Facebook, Inc.
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

package io.reactivesocket.tckdrivers.server;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.reactivesocket.tckdrivers.client.JavaTCPClient;
import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer;

@Command(name = "server", description = "server")
public class JavaTCPServer {

    @Option(name = "--port", description = "port")
    public static int port;

    @Option(name = "--file", description = "test file")
    public static String realfile;

    public static void main(String[] args) {

        SingleCommand<JavaTCPClient> cmd = SingleCommand.singleCommand(JavaTCPClient.class);
        cmd.parse(args);

        String file = "reactivesocket-tck-drivers/src/main/test/resources/servertest$.txt";

        if (realfile != null) {
            file = realfile;
        }

        JavaServerDriver jsd =
                new JavaServerDriver(file);

        TcpReactiveSocketServer.create(4567)
                .start((setupPayload, reactiveSocket) -> {
                    // create request handler
                    return jsd.parse();
                }).awaitShutdown();


    }

}
