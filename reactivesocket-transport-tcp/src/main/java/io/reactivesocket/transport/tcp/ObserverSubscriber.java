/*
 * Copyright 2016 Netflix, Inc.
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
 *
 */

package io.reactivesocket.transport.tcp;

import io.reactivesocket.Frame;
import io.reactivesocket.rx.Observer;
import rx.Subscriber;

public class ObserverSubscriber extends Subscriber<Frame> {

    private final Observer<Frame> o;

    public ObserverSubscriber(Observer<Frame> o) {
        this.o = o;
    }

    @Override
    public void onCompleted() {
        o.onComplete();
    }

    @Override
    public void onError(Throwable e) {
        o.onError(e);
    }

    @Override
    public void onNext(Frame frame) {
        o.onNext(frame);
    }
}
