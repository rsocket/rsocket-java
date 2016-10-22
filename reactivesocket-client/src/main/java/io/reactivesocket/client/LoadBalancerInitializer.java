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
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.internal.ValidatingSubscription;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This is a temporary class to provide a {@link LoadBalancingClient#connect()} implementation when {@link LoadBalancer}
 * does not support it.
 */
final class LoadBalancerInitializer implements Runnable {

    private volatile LoadBalancer loadBalancer;
    private final Publisher<ReactiveSocket> emitSource;
    private boolean ready; // Guarded by this.
    private final List<Subscriber<? super ReactiveSocket>> earlySubscribers = new CopyOnWriteArrayList<>();

    private LoadBalancerInitializer() {
        emitSource = s -> {
            final boolean _emit;
            synchronized (this) {
                _emit = ready;
                if (!_emit) {
                    earlySubscribers.add(s);
                }
            }
            if (_emit) {
                s.onSubscribe(ValidatingSubscription.empty(s));
                s.onNext(loadBalancer);
                s.onComplete();
            }
        };
    }

    static LoadBalancerInitializer create(Publisher<? extends Collection<ReactiveSocketClient>> factories) {
        final LoadBalancerInitializer initializer = new LoadBalancerInitializer();
        final LoadBalancer loadBalancer = new LoadBalancer(factories, initializer);
        initializer.loadBalancer = loadBalancer;
        return initializer;
    }

    Publisher<ReactiveSocket> connect() {
        return emitSource;
    }

    @Override
    public void run() {
        List<Subscriber<? super ReactiveSocket>> earlySubs;
        synchronized (this) {
            if (!ready) {
                earlySubs = new ArrayList<>(earlySubscribers);
                earlySubscribers.clear();
                ready = true;
            } else {
                return;
            }
        }
        Px<LoadBalancer> source = Px.just(loadBalancer);
        for (Subscriber<? super ReactiveSocket> earlySub : earlySubs) {
            source.subscribe(earlySub);
        }
    }

    synchronized double availability() {
        return ready? 1.0 : 0.0;
    }
}
