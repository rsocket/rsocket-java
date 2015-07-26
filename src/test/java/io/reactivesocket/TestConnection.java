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

import static rx.RxReactiveStreams.*;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import rx.Observable;
import rx.subjects.PublishSubject;

class TestConnection implements DuplexConnection {

    final PublishSubject<Message> toInput = PublishSubject.create();
    private PublishSubject<Message> writeSubject = PublishSubject.create();
    final Observable<Message> writes = writeSubject;

    @Override
    public Publisher<Void> write(Publisher<Message> o) {
        return toPublisher(toObservable(o).flatMap(m -> {
            // no backpressure on a Subject so just firehosing for this test
            writeSubject.onNext(m);
            return Observable.<Void> empty();
        }));
    }

    @Override
    public Publisher<Message> getInput() {
        return toPublisher(toInput);
    }
}