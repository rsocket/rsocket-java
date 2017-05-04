/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.aeron;

import io.rsocket.aeron.client.AeronTransportClient;
import io.rsocket.aeron.internal.AeronWrapper;
import io.rsocket.aeron.internal.Constants;
import io.rsocket.aeron.internal.DefaultAeronWrapper;
import io.rsocket.aeron.internal.EventLoop;
import io.rsocket.aeron.internal.SingleThreadedEventLoop;
import io.rsocket.aeron.internal.reactivestreams.AeronClientChannelConnector;
import io.rsocket.aeron.internal.reactivestreams.AeronSocketAddress;
import io.rsocket.aeron.server.AeronTransportServer;
import io.rsocket.lease.DisabledLeaseAcceptingSocket;
import io.rsocket.server.ReactiveSocketServer;
import io.rsocket.test.ClientSetupRule;
import io.rsocket.test.TestReactiveSocket;
import org.agrona.LangUtil;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;


class AeronClientSetupRule extends ClientSetupRule {
    static {
        MediaDriverHolder.getInstance();
        AeronWrapper aeronWrapper = new DefaultAeronWrapper();

        AeronSocketAddress serverManagementSocketAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        EventLoop serverEventLoop = new SingleThreadedEventLoop("server");
        server = new AeronTransportServer(aeronWrapper, serverManagementSocketAddress, serverEventLoop);

        // Create Client Connector
        AeronSocketAddress clientManagementSocketAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        EventLoop clientEventLoop = new SingleThreadedEventLoop("client");

        AeronSocketAddress receiveAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);
        AeronSocketAddress sendAddress = AeronSocketAddress.create("aeron:udp", "127.0.0.1", 39790);

        AeronClientChannelConnector.AeronClientConfig config = AeronClientChannelConnector
            .AeronClientConfig.create(
                receiveAddress,
                sendAddress,
                Constants.CLIENT_STREAM_ID,
                Constants.SERVER_STREAM_ID,
                clientEventLoop);

        AeronClientChannelConnector connector = AeronClientChannelConnector.create(aeronWrapper, clientManagementSocketAddress, clientEventLoop);


        client = new AeronTransportClient(connector, config);
    }


    private static final AeronTransportServer server;
    private static final AeronTransportClient client;

    AeronClientSetupRule() {
        super(
            socketAddress -> client,
            () ->
                ReactiveSocketServer.create(server)
                    .start((setup, sendingSocket) ->
                        new DisabledLeaseAcceptingSocket(
                            new TestReactiveSocket()))
            .getServerAddress()
        );
    }

    private static InetAddress getIPv4InetAddress() {
        InetAddress iaddress = null;
        try {
            String os = System.getProperty("os.name").toLowerCase();

            if (os.contains("nix") || os.contains("nux")) {
                NetworkInterface ni = NetworkInterface.getByName("eth0");

                Enumeration<InetAddress> ias = ni.getInetAddresses();

                do {
                    iaddress = ias.nextElement();
                } while (!(iaddress instanceof Inet4Address));

            }

            iaddress = InetAddress.getLocalHost();  // for Windows and OS X it should work well
        } catch (Exception e) {
            LangUtil.rethrowUnchecked(e);
        }

        return iaddress;
    }
}
