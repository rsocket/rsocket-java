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

package io.rsocket.transport.local;

import io.rsocket.RSocketFactory;
import io.rsocket.test.ClientSetupRule;
import io.rsocket.test.TestRSocket;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class LocalClientSetupRule extends ClientSetupRule<String> {
    private static final AtomicInteger uniqueNameGenerator = new AtomicInteger();

    public LocalClientSetupRule() {
        super(
            // This needs to be called twice before it increments
            // - once for the client and once for the server
            new Supplier<String>() {
                boolean increment = true;
                @Override
                public String get() {
                    if (increment) {
                        increment = false;
                        return "test" + uniqueNameGenerator.incrementAndGet();
                    } else {
                        increment = true;
                        return "test" + uniqueNameGenerator.get();
                    }
                }
            },
            address ->
                RSocketFactory
                    .connect()
                    .transport(LocalClientTransport.create(address))
                    .start()
                    .block(),
            address ->
                RSocketFactory
                    .receive()
                    .acceptor((setup, sendingSocket) -> Mono.just(new TestRSocket()))
                    .transport(LocalServerTransport.create(address))
                    .start()
                    .block()
        );
    }
}
