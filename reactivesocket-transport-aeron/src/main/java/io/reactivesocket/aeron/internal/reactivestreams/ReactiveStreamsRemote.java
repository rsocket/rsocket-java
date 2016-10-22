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
package io.reactivesocket.aeron.internal.reactivestreams;

import io.reactivesocket.reactivestreams.extensions.Px;
import org.reactivestreams.Publisher;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Interfaces to define a ReactiveStream over a remote channel
 */
public interface ReactiveStreamsRemote {
    interface In<T> extends Px<T> {
        static <T> In<T> from(Publisher<T> source) {
            if (source instanceof In) {
                return (In<T>) source;
            } else {
                return source::subscribe;
            }
        }
    }

    interface Out<T> extends Px<T> {
        static <T> Out<T> from(Publisher<T> source) {
            if (source instanceof Out) {
                return (Out<T>) source;
            } else {
                return source::subscribe;
            }
        }
    }

    interface Channel<T> {
        Publisher<Void> send(ReactiveStreamsRemote.In<? extends T> in);

        default Publisher<Void> send(T t) {
            return send(ReactiveStreamsRemote.In.from(Px.just(t)));
        }

        ReactiveStreamsRemote.Out<? extends T> receive();
        boolean isActive();
    }

    interface ClientChannelConnector<T extends ClientChannelConfig, R extends Channel<?>> extends Function<T, Publisher<R>> {}

    interface ClientChannelConfig {}

    interface ChannelConsumer<C extends Channel<?>> extends Consumer<C> {}

    abstract class ChannelServer<C extends ChannelConsumer<?>> {
        protected final C channelConsumer;

        public ChannelServer(C channelConsumer) {
            this.channelConsumer = channelConsumer;
        }

        public abstract StartedServer start();
    }

    interface StartedServer {
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
         * Blocks till this server shutsdown. <p>
         *     <em>This does not shutdown the server.</em>
         */
        void awaitShutdown();

        /**
         * Blocks till this server shutsdown till the passed duration. <p>
         *     <em>This does not shutdown the server.</em>
         */
        void awaitShutdown(long duration, TimeUnit durationUnit);

        /**
         * Initiates the shutdown of this server.
         */
        void shutdown();
    }

}
