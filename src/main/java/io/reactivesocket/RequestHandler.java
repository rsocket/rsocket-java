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

import java.util.function.Function;

import org.reactivestreams.Publisher;

public abstract class RequestHandler {

    // TODO replace String with whatever ByteBuffer/byte[]/ByteBuf/etc variant we choose
    
    public abstract Publisher<Payload> handleRequestResponse(final Payload payload);

    public abstract Publisher<Payload> handleRequestStream(final Payload payload);

    public abstract Publisher<Payload> handleRequestSubscription(final Payload payload);

    public abstract Publisher<Void> handleFireAndForget(final Payload payload);

    public static RequestHandler create(
            Function<Payload, Publisher<Payload>> requestResponseHandler,
            Function<Payload, Publisher<Payload>> requestStreamHandler,
            Function<Payload, Publisher<Payload>> requestSubscriptionHandler,
            Function<Payload, Publisher<Void>> fireAndForgetHandler) {
        return new RequestHandler() {

            @Override
            public Publisher<Payload> handleRequestResponse(final Payload payload) {
                return requestResponseHandler.apply(payload);
            }

            @Override
            public Publisher<Payload> handleRequestStream(final Payload payload) {
                return requestStreamHandler.apply(payload);
            }

            @Override
            public Publisher<Payload> handleRequestSubscription(final Payload payload) {
                return requestSubscriptionHandler.apply(payload);
            }

            @Override
            public Publisher<Void> handleFireAndForget(final Payload payload) {
                return fireAndForgetHandler.apply(payload);
            }

        };
    }
}
