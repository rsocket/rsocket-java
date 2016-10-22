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

package io.reactivesocket.reactivestreams.extensions;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSubscriber<T> implements Subscriber<T> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSubscriber.class);

    @SuppressWarnings("rawtypes")
    private static final Subscriber defaultInstance = new DefaultSubscriber();

    @Override
    public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(T o) {
        if (logger.isDebugEnabled()) {
            logger.debug("Next emission reached the defaul subscriber. Item => " + o);
        }
    }

    @Override
    public void onError(Throwable t) {
        logger.error("Uncaught error emitted.", t);
    }

    @Override
    public void onComplete() {

    }

    @SuppressWarnings("unchecked")
    public static <R> Subscriber<R> defaultInstance() {
        return defaultInstance;
    }
}
