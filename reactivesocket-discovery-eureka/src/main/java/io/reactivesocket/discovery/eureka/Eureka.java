package io.reactivesocket.discovery.eureka;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.CacheRefreshedEvent;
import com.netflix.discovery.EurekaClient;
import io.reactivesocket.internal.rx.EmptySubscription;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.stream.Collectors;

public class Eureka {
    private EurekaClient client;

    public Eureka(EurekaClient client) {
        this.client = client;
    }

    public Publisher<List<SocketAddress>> subscribeToAsg(String vip, boolean secure) {
        return new Publisher<List<SocketAddress>>() {
            @Override
            public void subscribe(Subscriber<? super List<SocketAddress>> subscriber) {
                // TODO: backpressure
                subscriber.onSubscribe(EmptySubscription.INSTANCE);
                pushChanges(subscriber);

                client.registerEventListener(event -> {
                    if (event instanceof CacheRefreshedEvent) {
                        pushChanges(subscriber);
                    }
                });
            }

            private synchronized void pushChanges(Subscriber<? super List<SocketAddress>> subscriber) {
                List<InstanceInfo> infos = client.getInstancesByVipAddress(vip, secure);
                List<SocketAddress> socketAddresses = infos.stream()
                    .map(info -> {
                        String ip = info.getIPAddr();
                        int port = secure ? info.getSecurePort() : info.getPort();
                        return InetSocketAddress.createUnresolved(ip, port);
                    })
                    .collect(Collectors.toList());
                subscriber.onNext(socketAddresses);
            }
        };
    }
}
