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
package io.rsocket.internal;

import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public final class KnownErrorFilter implements Consumer<Throwable> {

    private static final List<Class<? extends Throwable>> knownErrors =
            Collections.singletonList(ClosedChannelException.class);
    private final Consumer<Throwable> delegate;

    public KnownErrorFilter(Consumer<Throwable> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void accept(Throwable throwable) {
        if (!knownErrors.contains(throwable.getClass())) {
            delegate.accept(throwable);
        }
    }
}
