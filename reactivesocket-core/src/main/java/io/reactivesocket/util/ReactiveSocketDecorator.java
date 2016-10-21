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

package io.reactivesocket.util;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import org.reactivestreams.Publisher;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A utility class to decorate parts of the API of an existing {@link ReactiveSocket}.<p>
 *     All methods mutate state, hence, this class is <em>not</em> thread-safe.
 */
public class ReactiveSocketDecorator {

    private Function<Payload, Publisher<Payload>> reqResp;
    private Function<Payload, Publisher<Payload>> reqStream;
    private Function<Payload, Publisher<Payload>> reqSub;
    private Function<Publisher<Payload>, Publisher<Payload>> reqChannel;
    private Function<Payload, Publisher<Void>> fnf;
    private Function<Payload, Publisher<Void>> metaPush;
    private Supplier<Double> availability;
    private Supplier<Publisher<Void>> close;
    private Supplier<Publisher<Void>> onClose;

    private final ReactiveSocket delegate;

    private ReactiveSocketDecorator(ReactiveSocket delegate) {
        this.delegate = delegate;
        reqResp = payload -> delegate.requestResponse(payload);
        reqStream = payload -> delegate.requestStream(payload);
        reqSub = payload -> delegate.requestSubscription(payload);
        reqChannel = payload -> delegate.requestChannel(payload);
        fnf = payload -> delegate.fireAndForget(payload);
        metaPush = payload -> delegate.metadataPush(payload);
        availability = () -> delegate.availability();
        close = () -> delegate.close();
        onClose = () -> delegate.onClose();
    }

    public ReactiveSocket finish() {
        return new ReactiveSocket() {
            @Override
            public Publisher<Void> fireAndForget(Payload payload) {
                return fnf.apply(payload);
            }

            @Override
            public Publisher<Payload> requestResponse(Payload payload) {
                return reqResp.apply(payload);
            }

            @Override
            public Publisher<Payload> requestStream(Payload payload) {
                return reqStream.apply(payload);
            }

            @Override
            public Publisher<Payload> requestSubscription(Payload payload) {
                return reqSub.apply(payload);
            }

            @Override
            public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
                return reqChannel.apply(payloads);
            }

            @Override
            public Publisher<Void> metadataPush(Payload payload) {
                return metaPush.apply(payload);
            }

            @Override
            public Publisher<Void> close() {
                return close.get();
            }

            @Override
            public Publisher<Void> onClose() {
                return onClose.get();
            }

            @Override
            public double availability() {
                return availability.get();
            }
        };
    }

    /**
     * Decorates underlying {@link ReactiveSocket#requestResponse(Payload)} with the provided mapping function.
     *
     * @param responseMapper Mapper used to decorate the response of {@link ReactiveSocket#requestResponse(Payload)}.
     * Input to the function is the original response of the underlying {@code ReactiveSocket}
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator requestResponse(Function<Publisher<Payload>, Publisher<Payload>> responseMapper) {
        reqResp = payload -> responseMapper.apply(delegate.requestResponse(payload));
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#requestResponse(Payload)} with the provided mapping function.
     *
     * @param mapper Mapper used to override the call to the underlying {@code ReactiveSocket}. First argument here is
     * the payload for request and the socket passed is the underlying {@code ReactiveSocket}.
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator requestResponse(BiFunction<Payload, ReactiveSocket, Publisher<Payload>> mapper) {
        reqResp = payload -> mapper.apply(payload, delegate);
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#requestStream(Payload)} with the provided mapping function.
     *
     * @param responseMapper Mapper used to decorate the response of {@link ReactiveSocket#requestStream(Payload)}.
     * Input to the function is the original response of the underlying {@code ReactiveSocket}
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator requestStream(Function<Publisher<Payload>, Publisher<Payload>> responseMapper) {
        reqStream = payload -> responseMapper.apply(delegate.requestStream(payload));
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#requestStream(Payload)} with the provided mapping function.
     *
     * @param mapper Mapper used to override the call to the underlying {@code ReactiveSocket}. First argument here is
     * the payload for request and the socket passed is the underlying {@code ReactiveSocket}.
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator requestStream(BiFunction<Payload, ReactiveSocket, Publisher<Payload>> mapper) {
        reqStream = payload -> mapper.apply(payload, delegate);
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#requestSubscription(Payload)} with the provided mapping function.
     *
     * @param responseMapper Mapper used to decorate the response of {@link ReactiveSocket#requestSubscription(Payload)}.
     * Input to the function is the original response of the underlying {@code ReactiveSocket}
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator requestSubscription(Function<Publisher<Payload>, Publisher<Payload>> responseMapper) {
        reqSub = payload -> responseMapper.apply(delegate.requestSubscription(payload));
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#requestSubscription(Payload)} with the provided mapping function.
     *
     * @param mapper Mapper used to override the call to the underlying {@code ReactiveSocket}. First argument here is
     * the payload for request and the socket passed is the underlying {@code ReactiveSocket}.
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator requestSubscription(BiFunction<Payload, ReactiveSocket, Publisher<Payload>> mapper) {
        reqSub = payload -> mapper.apply(payload, delegate);
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#requestChannel(Publisher)} with the provided mapping function.
     *
     * @param responseMapper Mapper used to decorate the response of {@link ReactiveSocket#requestChannel(Publisher)}.
     * Input to the function is the original response of the underlying {@code ReactiveSocket}
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator requestChannel(Function<Publisher<Payload>, Publisher<Payload>> responseMapper) {
        reqChannel = payload -> responseMapper.apply(delegate.requestChannel(payload));
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#requestChannel(Publisher)} with the provided mapping function.
     *
     * @param mapper Mapper used to override the call to the underlying {@code ReactiveSocket}. First argument here is
     * the payload for request and the socket passed is the underlying {@code ReactiveSocket}.
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator requestChannel(BiFunction<Publisher<Payload>, ReactiveSocket, Publisher<Payload>> mapper) {
        reqChannel = payloads -> mapper.apply(payloads, delegate);
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#fireAndForget(Payload)} with the provided mapping function.
     *
     * @param responseMapper Mapper used to decorate the response of {@link ReactiveSocket#fireAndForget(Payload)}.
     * Input to the function is the original response of the underlying {@code ReactiveSocket}
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator fireAndForget(Function<Publisher<Void>, Publisher<Void>> responseMapper) {
        fnf = payload -> responseMapper.apply(delegate.fireAndForget(payload));
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#fireAndForget(Payload)} with the provided mapping function.
     *
     * @param mapper Mapper used to override the call to the underlying {@code ReactiveSocket}. First argument here is
     * the payload for request and the socket passed is the underlying {@code ReactiveSocket}.
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator fireAndForget(BiFunction<Payload, ReactiveSocket, Publisher<Void>> mapper) {
        fnf = payloads -> mapper.apply(payloads, delegate);
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#metadataPush(Payload)} with the provided mapping function.
     *
     * @param responseMapper Mapper used to decorate the response of {@link ReactiveSocket#metadataPush(Payload)}.
     * Input to the function is the original response of the underlying {@code ReactiveSocket}
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator metadataPush(Function<Publisher<Void>, Publisher<Void>> responseMapper) {
        metaPush = payload -> responseMapper.apply(delegate.metadataPush(payload));
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#metadataPush(Payload)} with the provided mapping function.
     *
     * @param mapper Mapper used to override the call to the underlying {@code ReactiveSocket}. First argument here is
     * the payload for request and the socket passed is the underlying {@code ReactiveSocket}.
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator metadataPush(BiFunction<Payload, ReactiveSocket, Publisher<Void>> mapper) {
        metaPush = payloads -> mapper.apply(payloads, delegate);
        return this;
    }

    /**
     * Decorates all responses of the underlying {@code ReactiveSocket} with the provided mapping function. This will
     * only decorate {@link ReactiveSocket#requestResponse(Payload)}, {@link ReactiveSocket#requestStream(Payload)},
     * {@link ReactiveSocket#requestSubscription(Payload)} and {@link ReactiveSocket#requestChannel(Publisher)}.
     *
     * @param mapper Mapper used to override the call to the underlying {@code ReactiveSocket}. First argument here is
     * the original response.
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator decorateAllResponses(Function<Publisher<Payload>, Publisher<Payload>> mapper) {
        requestResponse(resp -> mapper.apply(resp)).requestStream(resp -> mapper.apply(resp))
                                                   .requestSubscription(resp -> mapper.apply(resp))
                                                   .requestChannel(resp -> mapper.apply(resp));
        return this;
    }

    /**
     * Decorates all responses of the underlying {@code ReactiveSocket} with the provided mapping function. This will
     * only decorate {@link ReactiveSocket#metadataPush(Payload)} and {@link ReactiveSocket#fireAndForget(Payload)}.
     *
     * @param mapper Mapper used to override the call to the underlying {@code ReactiveSocket}. First argument here is
     * the original response.
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator decorateAllVoidResponses(Function<Publisher<Void>, Publisher<Void>> mapper) {
        fireAndForget(resp -> mapper.apply(resp)).metadataPush(resp -> mapper.apply(resp));
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#close()} with the provided mapping function.
     *
     * @param mapper Mapper used to override the call to the underlying {@code ReactiveSocket}. Argument here is the
     * underlying {@code ReactiveSocket}.
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator close(Function<ReactiveSocket, Publisher<Void>> mapper) {
        close = () -> mapper.apply(delegate);
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#onClose()} with the provided mapping function.
     *
     * @param mapper Mapper used to override the call to the underlying {@code ReactiveSocket}. Argument here is the
     * underlying {@code ReactiveSocket}.
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator onClose(Function<ReactiveSocket, Publisher<Void>> mapper) {
        onClose = () -> mapper.apply(delegate);
        return this;
    }

    /**
     * Decorates underlying {@link ReactiveSocket#availability()} with the provided mapping function.
     *
     * @param mapper Mapper used to override the call to the underlying {@code ReactiveSocket}. Argument here is the
     * underlying {@code ReactiveSocket}.
     *
     * @return {@code this}
     */
    public ReactiveSocketDecorator availability(Function<ReactiveSocket, Double> mapper) {
        availability = () -> mapper.apply(delegate);
        return this;
    }

    /**
     * Starts wrapping the passed {@code source}. Use instance level methods to decorate the APIs.
     *
     * @param source Source socket to decorate.
     *
     * @return A new {@code ReactiveSocketDecorator} instance.
     */
    public static ReactiveSocketDecorator wrap(ReactiveSocket source) {
        return new ReactiveSocketDecorator(source);
    }

    /**
     * Starts with a {@code ReactiveSocket} that rejects all requests. Use instance level methods to decorate the APIs.
     *
     * @return A new {@code ReactiveSocketDecorator} instance.
     */
    public static ReactiveSocketDecorator empty() {
        return new ReactiveSocketDecorator(new AbstractReactiveSocket() { });
    }
}
