/**
 * Copyright 2016 Netflix, Inc.
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
package io.reactivesocket.aeron.client;

import io.reactivesocket.*;
import io.reactivesocket.rx.Completable;
import org.agrona.LangUtil;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.RxReactiveStreams;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.Enumeration;
import java.util.function.Consumer;

/**
 * An implementation of {@link ReactiveSocketFactory} that creates Aeron ReactiveSockets.
 */
public class AeronReactiveSocketConnector implements ReactiveSocketConnector<SocketAddress> {
    private static final Logger logger = LoggerFactory.getLogger(AeronReactiveSocketConnector.class);

    private final ConnectionSetupPayload connectionSetupPayload;
    private final Consumer<Throwable> errorStream;

    public AeronReactiveSocketConnector(ConnectionSetupPayload connectionSetupPayload, Consumer<Throwable> errorStream) {
        this(getIPv4InetAddress().getHostAddress(), 39790, connectionSetupPayload, errorStream);
    }

    public AeronReactiveSocketConnector(String host, int port, ConnectionSetupPayload connectionSetupPayload, Consumer<Throwable> errorStream) {
        this.connectionSetupPayload = connectionSetupPayload;
        this.errorStream = errorStream;

        try {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(host, port);
            logger.info("Listen to ReactiveSocket Aeron response on host {} port {}", host, port);
            AeronClientDuplexConnectionFactory.getInstance().addSocketAddressToHandleResponses(inetSocketAddress);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            LangUtil.rethrowUnchecked(e);
        }
    }

    @Override
    public Publisher<ReactiveSocket> connect(SocketAddress address) {
        Publisher<AeronClientDuplexConnection> connection
            = AeronClientDuplexConnectionFactory.getInstance().createAeronClientDuplexConnection(address);

        Observable<ReactiveSocket> result = Observable.create(s ->
            connection.subscribe(new Subscriber<AeronClientDuplexConnection>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(AeronClientDuplexConnection connection) {
                    ReactiveSocket reactiveSocket = DefaultReactiveSocket.fromClientConnection(connection, connectionSetupPayload, errorStream);
                    reactiveSocket.start(new Completable() {
                        @Override
                        public void success() {
                            s.onNext(reactiveSocket);
                            s.onCompleted();
                        }

                        @Override
                        public void error(Throwable e) {
                            s.onError(e);
                        }
                    });
                }

                @Override
                public void onError(Throwable t) {
                    s.onError(t);
                }

                @Override
                public void onComplete() {
                }
            })
        );

        return RxReactiveStreams.toPublisher(result);
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
            logger.error(e.getMessage(), e);
            LangUtil.rethrowUnchecked(e);
        }

        return iaddress;
    }
}
