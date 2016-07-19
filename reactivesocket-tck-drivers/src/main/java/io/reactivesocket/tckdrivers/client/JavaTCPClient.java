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

import io.netty.channel.nio.NioEventLoopGroup;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DefaultReactiveSocket;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.transport.tcp.TcpDuplexConnection;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.RxReactiveStreams;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;

import static java.net.InetSocketAddress.createUnresolved;

// this client should parse the test cases we wrote and use them to
public class JavaTCPClient {

    public static void main(String[] args) throws MalformedURLException, URISyntaxException {
        // we pass in our reactive socket here to the test suite
        String file = "/Users/mjzhu/dev/reactivesocket-java/reactivesocket-tck-drivers/clienttest$.txt";
        if (args.length > 0) {
            file = args[0];
        }
        try {
            JavaClientDriver jd = new JavaClientDriver(file);
            jd.runTests();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Publisher<ReactiveSocket> buildConnection(URI uri) {
        if (uri.getScheme().equals("tcp")) {
            ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.HONOR_LEASE);

            TcpReactiveSocketConnector tcp = TcpReactiveSocketConnector.create(setupPayload, Throwable::printStackTrace);
            Publisher<ReactiveSocket> socketPublisher = tcp.connect(new InetSocketAddress("localhost", 4567));
            return socketPublisher;

        } else {
            throw new UnsupportedOperationException("uri unsupported: " + uri);
        }
    }

    public static ReactiveSocket createClient() {
        try {
            String target = "tcp://localhost:4567/rs";
            URI uri = new URI(target);

            Publisher<ReactiveSocket> socketPublisher = buildConnection(uri);
            SocketWrapper client = new SocketWrapper();
            CountDownLatch hold = new CountDownLatch(1);
            socketPublisher.subscribe(new Subscriber<ReactiveSocket>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(ReactiveSocket reactiveSocket) {
                    client.setSocket(reactiveSocket);
                    hold.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    System.out.println("error when getting reactive socket");
                }

                @Override
                public void onComplete() {

                }
            });
            hold.await();
            return client.getSocket();
        } catch (Exception e) {
            System.out.println("Something went wrong");
        }
        return null;
    }

    private static class SocketWrapper {

        private ReactiveSocket rs;

        public void setSocket(ReactiveSocket rs) {
            this.rs = rs;
        }

        public ReactiveSocket getSocket() {
            return this.rs;
        }
    }

}
