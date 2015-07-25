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

import org.reactivestreams.Publisher;

import rx.functions.Func1;

public abstract class RequestHandler {

    // TODO replace String with whatever ByteBuffer/byte[]/ByteBuf/etc variant we choose
    
    public abstract Publisher<String> handleRequestResponse(String request);

    public abstract Publisher<String> handleRequestStream(String request);

    public abstract Publisher<String> handleRequestSubscription(String request);

    public abstract Publisher<Void> handleFireAndForget(String request);

    public static RequestHandler create(
            Func1<String, Publisher<String>> requestResponseHandler,
            Func1<String, Publisher<String>> requestStreamHandler,
            Func1<String, Publisher<String>> requestSubscriptionHandler,
            Func1<String, Publisher<Void>> fireAndForgetHandler) {
        return new RequestHandler() {

            @Override
            public Publisher<String> handleRequestResponse(String request) {
                return requestResponseHandler.call(request);
            }

            @Override
            public Publisher<String> handleRequestStream(String request) {
                return requestStreamHandler.call(request);
            }

            @Override
            public Publisher<String> handleRequestSubscription(String request) {
                return requestSubscriptionHandler.call(request);
            }

            @Override
            public Publisher<Void> handleFireAndForget(String request) {
                return fireAndForgetHandler.call(request);
            }

        };
    }
}
