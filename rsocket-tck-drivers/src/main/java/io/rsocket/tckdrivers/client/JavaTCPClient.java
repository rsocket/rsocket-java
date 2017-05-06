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

package io.rsocket.tckdrivers.client;

import io.rsocket.RSocket;
import io.rsocket.transport.netty.client.TcpTransportClient;

import io.rsocket.client.RSocketClient;
import io.reactivex.Flowable;
import io.rsocket.client.KeepAliveProvider;
import io.rsocket.client.SetupProvider;

import reactor.ipc.netty.tcp.TcpClient;

import java.net.*;
import java.util.List;


/**
 * A client that implements a method to create RSockets, and runs the tests.
 */
public class JavaTCPClient {

    private static URI uri;

    public boolean run(String realfile, String host, int port, boolean debug2, List<String> tests)
            throws MalformedURLException, URISyntaxException {
        // we pass in our reactive socket here to the test suite
        String file = "rsocket-tck-drivers/src/test/resources/clienttest.txt";
        if (realfile != null) file = realfile;
        try {
            setURI(new URI("tcp://" + host + ":" + port + "/rs"));
            JavaClientDriver jd = new JavaClientDriver(file, JavaTCPClient::createClient, tests);
            return jd.runTests();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void setURI(URI uri2) {
        uri = uri2;
    }

    /**
     * A function that creates a RSocket on a new TCP connection.
     * @return a RSocket
     */
    public static RSocket createClient() {
        if ("tcp".equals(uri.getScheme())) {
            SocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
            RSocketClient client = RSocketClient.create(TcpTransportClient.create(TcpClient.create(options ->
            options.connect((InetSocketAddress)address))),
                    SetupProvider.keepAlive(KeepAliveProvider.never()).disableLease());
            RSocket socket = Flowable.fromPublisher(client.connect()).singleOrError().blockingGet();
            return socket;
        }
        else {
            throw new UnsupportedOperationException("uri unsupported: " + uri);
        }
    }

}
