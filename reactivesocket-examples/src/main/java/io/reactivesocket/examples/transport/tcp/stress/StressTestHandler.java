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
import reactor.core.publisher.Mono;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

class StressTestHandler extends AbstractReactiveSocket {

    private final Supplier<Result> failureSelector;

    private StressTestHandler(Supplier<Result> failureSelector) {
        this.failureSelector = failureSelector;
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return Mono.defer(() -> {
            Result result = failureSelector.get();
            switch (result) {
            case Fail:
                return Mono.error(new Exception("SERVER EXCEPTION"));
            case DontReply:
                return Mono.never(); // Cause timeout
            default:
                return Mono.just(new PayloadImpl("Response"));
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
