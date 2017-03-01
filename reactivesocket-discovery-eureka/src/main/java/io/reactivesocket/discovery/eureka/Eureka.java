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

package io.reactivesocket.discovery.eureka;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.CacheRefreshedEvent;
import com.netflix.discovery.EurekaClient;
import io.reactivesocket.internal.ValidatingSubscription;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class Eureka {
    private EurekaClient client;

    public Eureka(EurekaClient client) {
        this.client = client;
    }

    public Publisher<Collection<SocketAddress>> subscribeToAsg(String vip, boolean secure) {
        return new Publisher<Collection<SocketAddress>>() {
            @Override
            public void subscribe(Subscriber<? super Collection<SocketAddress>> subscriber) {
                // TODO: backpressure
                subscriber.onSubscribe(ValidatingSubscription.empty(subscriber));
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
                    .filter(instanceInfo -> instanceInfo.getStatus() == InstanceStatus.UP)
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
