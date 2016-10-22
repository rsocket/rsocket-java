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

import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * A contract used to schedule tasks in future.
 */
public interface Scheduler {

    /**
     * A single tick timer which returns a {@code Publisher} that completes when the passed {@code time} is elapsed.
     *
     * @param time Time at which the timer will tick.
     * @param unit {@code TimeUnit} for the time.
     *
     * @return A {@code Publisher} that completes when the time is elapsed.
     */
    Publisher<Void> timer(long time, TimeUnit unit);

}
