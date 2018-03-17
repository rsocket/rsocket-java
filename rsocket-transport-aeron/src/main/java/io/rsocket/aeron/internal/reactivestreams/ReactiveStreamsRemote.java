/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.aeron.internal.reactivestreams;

import io.rsocket.Closeable;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Interfaces to define a ReactiveStream over a remote channel */
public interface ReactiveStreamsRemote {
  interface Channel<T> {
    Mono<Void> send(Flux<? extends T> in);

    default Mono<Void> send(T t) {
      return send(Flux.just(t));
    }

    Flux<? extends T> receive();
  }

  interface ClientChannelConnector<T extends ClientChannelConfig, R extends Channel<?>>
      extends Function<T, Publisher<R>> {}

  interface ClientChannelConfig {}

  interface ChannelConsumer<C extends Channel<?>> extends Consumer<C> {}

  abstract class ChannelServer<C extends ChannelConsumer<?>> {
    protected final C channelConsumer;

    public ChannelServer(C channelConsumer) {
      this.channelConsumer = channelConsumer;
    }

    public abstract StartedServer start();
  }

  interface StartedServer extends Closeable {
    /**
     * Address for this server.
     *
     * @return Address for this server.
     */
    SocketAddress getServerAddress();

    /**
     * Port for this server.
     *
     * @return Port for this server.
     */
    int getServerPort();

    /**
     * Blocks till this server shutsdown.
     *
     * <p><em>This does not shutdown the server.</em>
     */
    void awaitShutdown();

    /**
     * Blocks till this server shutsdown till the passed duration.
     *
     * <p><em>This does not shutdown the server.</em>
     *
     * @param duration the number of durationUnit to wait
     * @param durationUnit the unit e.g. seconds
     */
    void awaitShutdown(long duration, TimeUnit durationUnit);

    /** Initiates the shutdown of this server. */
    void shutdown();
  }
}
