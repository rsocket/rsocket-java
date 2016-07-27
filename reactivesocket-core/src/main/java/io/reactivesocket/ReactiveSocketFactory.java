/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket;

import io.reactivesocket.util.ReactiveSocketFactoryProxy;
import org.reactivestreams.Publisher;
import java.util.function.Function;

/**
 * Factory of ReactiveSocket interface
 * This abstraction is useful for abstracting the creation of a ReactiveSocket
 * (e.g. inside the LoadBalancer which create ReactiveSocket as needed)
 */
public interface ReactiveSocketFactory {

    /**
     * Construct the ReactiveSocket.
     *
     * @return A source that emits a single {@code ReactiveSocket}.
     */
    Publisher<ReactiveSocket> apply();

    /**
     * @return a positive numbers representing the availability of the factory.
     * Higher is better, 0.0 means not available
     */
    double availability();

    default ReactiveSocketFactory chain(Function<Publisher<ReactiveSocket>, Publisher<ReactiveSocket>> conversion) {
        return new ReactiveSocketFactoryProxy(ReactiveSocketFactory.this) {
            @Override
            public Publisher<ReactiveSocket> apply() {
                return conversion.apply(super.apply());
            }
        };
    }
}
