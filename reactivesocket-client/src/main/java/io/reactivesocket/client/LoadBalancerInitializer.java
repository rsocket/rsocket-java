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

package io.reactivesocket.client;

import io.reactivesocket.ReactiveSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.Collection;

/**
 * This is a temporary class to provide a {@link LoadBalancingClient#connect()} implementation when {@link LoadBalancer}
 * does not support it.
 */
final class LoadBalancerInitializer implements ReactiveSocketClient, Runnable {

    private final LoadBalancer loadBalancer;
    private final MonoProcessor<ReactiveSocket> emitSource = MonoProcessor.create();

    private LoadBalancerInitializer(Publisher<? extends Collection<ReactiveSocketClient>> factories) {
        loadBalancer = new LoadBalancer(factories, this);
    }

    static LoadBalancerInitializer create(Publisher<? extends Collection<ReactiveSocketClient>> factories) {
        return new LoadBalancerInitializer(factories);
    }

    public Mono<ReactiveSocket> connect() {
        return emitSource;
    }

    @Override
    public void run() {
        emitSource.onNext(loadBalancer);
    }

    public synchronized double availability() {
        return emitSource.isTerminated() ? 1.0 : 0.0;
    }
}
