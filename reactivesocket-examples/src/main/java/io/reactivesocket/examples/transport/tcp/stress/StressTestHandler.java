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

package io.reactivesocket.examples.transport.tcp.stress;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

class StressTestHandler extends AbstractReactiveSocket {

    private final Callable<Result> failureSelector;

    private StressTestHandler(Callable<Result> failureSelector) {
        this.failureSelector = failureSelector;
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return Flowable.defer(() -> {
            Result result = failureSelector.call();
            switch (result) {
            case Fail:
                return Flowable.error(new Exception("SERVER EXCEPTION"));
            case DontReply:
                return Flowable.never(); // Cause timeout
            default:
                return Flowable.just(new PayloadImpl("Response"));
            }
        });
    }

    public static ReactiveSocket alwaysPass() {
        return new StressTestHandler(() -> Result.Pass);
    }

    public static ReactiveSocket randomFailuresAndDelays() {
        return new StressTestHandler(() -> {
            if (ThreadLocalRandom.current().nextInt(2) == 0) {
                return Result.Fail;
            }
            return Result.DontReply;
        });
    }

    public enum Result {
        Fail,
        DontReply,
        Pass
    }
}
