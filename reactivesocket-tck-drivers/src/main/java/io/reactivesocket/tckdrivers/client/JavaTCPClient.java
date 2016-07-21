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

package io.reactivesocket.tckdrivers.client;

import io.airlift.airline.*;
import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.tckdrivers.test.Main;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import io.reactivex.Single;
import io.reactivex.netty.protocol.tcp.client.TcpClient;

import java.net.*;
import java.util.function.Function;

import static rx.RxReactiveStreams.toObservable;

// this client should parse the test cases we wrote and use them to
@Command(name = "client", description = "client")
public class JavaTCPClient {

    public static URI uri;
    @Option(name = "--debug", description = "Turns on Frame Level comments")
    public static boolean debug;

    @Option(name = "--host", description = "Host name")
    public static String host;

    @Option(name = "--port", description = "port")
    public static int port;

    @Option(name = "--file", description = "test file")
    public static String realfile;

    public static void main(String[] args) throws MalformedURLException, URISyntaxException {

        SingleCommand<JavaTCPClient> cmd = SingleCommand.singleCommand(JavaTCPClient.class);
        cmd.parse(args);
        // we pass in our reactive socket here to the test suite
        String file = "reactivesocket-tck-drivers/src/main/test/resources/clienttest$.txt";
        if (realfile != null) file = realfile;
        try {
            setURI(new URI("tcp://" + host + ":" + port + "/rs"));
            JavaClientDriver jd = new JavaClientDriver(file);
            jd.runTests();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void setURI(URI uri2) {
        uri = uri2;
    }

    public static ReactiveSocket createClient() {
        ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create("", "");

        if ("tcp".equals(uri.getScheme())) {
            Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory =
                    socketAddress -> TcpClient.newClient(socketAddress);

            if (debug) clientFactory =
                    socketAddress -> TcpClient.newClient(socketAddress).enableWireLogging("rs",
                            LogLevel.ERROR);

            return toObservable(
                    TcpReactiveSocketConnector.create(setupPayload, Throwable::printStackTrace, clientFactory)
                            .connect(new InetSocketAddress(uri.getHost(), uri.getPort()))).toSingle()
                    .toBlocking()
                    .value();
        }
        else {
            throw new UnsupportedOperationException("uri unsupported: " + uri);
        }
    }

}
