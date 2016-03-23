package io.reactivesocket.aeron.client;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.real_logic.agrona.LangUtil;

import java.net.*;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * An implementation of {@link ReactiveSocketFactory} that creates Aeron ReactiveSockets.
 */
public class AeronReactiveSocketFactory implements ReactiveSocketFactory {
    private static final Logger logger = LoggerFactory.getLogger(AeronReactiveSocketFactory.class);

    private final ConnectionSetupPayload connectionSetupPayload;
    private final Consumer<Throwable> errorStream;

    public AeronReactiveSocketFactory(ConnectionSetupPayload connectionSetupPayload, Consumer<Throwable> errorStream) {
        this(getIPv4InetAddress().getHostAddress(), 39790, connectionSetupPayload, errorStream);
    }

    public AeronReactiveSocketFactory(String host, int port, ConnectionSetupPayload connectionSetupPayload, Consumer<Throwable> errorStream) {
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
    public Publisher<ReactiveSocket> call(SocketAddress address, long timeout, TimeUnit timeUnit) {
        Publisher<AeronClientDuplexConnection> aeronClientDuplexConnection
            = AeronClientDuplexConnectionFactory.getInstance().createAeronClientDuplexConnection(address);

        return (Subscriber<? super ReactiveSocket> s) -> {
            s.onSubscribe(EmptySubscription.INSTANCE);
            aeronClientDuplexConnection
                .subscribe(new Subscriber<AeronClientDuplexConnection>() {

                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(AeronClientDuplexConnection connection) {
                        ReactiveSocket reactiveSocket = ReactiveSocket.fromClientConnection(connection, connectionSetupPayload, errorStream);
                        CountDownLatch latch = new CountDownLatch(1);
                        reactiveSocket.start(new Completable() {
                            @Override
                            public void success() {
                                latch.countDown();
                                s.onNext(reactiveSocket);
                                s.onComplete();
                            }

                            @Override
                            public void error(Throwable e) {
                                s.onError(e);
                            }
                        });

                        try {
                            latch.await(timeout, timeUnit);
                        } catch (InterruptedException e) {
                            logger.error(e.getMessage(), e);
                            s.onError(e);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        s.onError(t);
                    }

                    @Override
                    public void onComplete() {
                    }
                });
        };
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
