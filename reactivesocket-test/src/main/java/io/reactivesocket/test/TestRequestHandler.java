/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.test;

import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.exceptions.UnsupportedSetupException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;

import static rx.RxReactiveStreams.*;

public class TestRequestHandler extends RequestHandler {

    @Override
    public Publisher<Payload> handleRequestResponse(Payload payload) {
        return toPublisher(Observable.just(TestUtil.utf8EncodedPayload("hello world", "metadata")));
    }

    @Override
    public Publisher<Payload> handleRequestStream(Payload payload) {
        return toPublisher(toObservable(handleRequestResponse(payload)).repeat(10));
    }

    @Override
    public Publisher<Payload> handleSubscription(Payload payload) {
        return toPublisher(toObservable(handleRequestStream(payload)).repeat());
    }

    @Override
    public Publisher<Void> handleFireAndForget(Payload payload) {
        return Subscriber::onComplete;
    }

    @Override
    public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> inputs) {
        return toPublisher(Observable.error(new UnsupportedSetupException("Channel not supported.")));
    }

    @Override
    public Publisher<Void> handleMetadataPush(Payload payload) {
        return toPublisher(Observable.error(new UnsupportedSetupException("Metadata push not supported.")));
    }
}
