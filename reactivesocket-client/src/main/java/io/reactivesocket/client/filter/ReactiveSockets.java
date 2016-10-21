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

package io.reactivesocket.client.filter;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.Scheduler;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import io.reactivesocket.util.ReactiveSocketDecorator;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public final class ReactiveSockets {

    private ReactiveSockets() {
        // No Instances.
    }

    /**
     * Provides a mapping function to wrap a {@code ReactiveSocket} such that all requests will timeout, if not
     * completed after the specified {@code timeout}.
     *
     * @param timeout timeout duration.
     * @param unit timeout duration unit.
     * @param scheduler scheduler for timeout.
     *
     * @return Function to transform any socket into a timeout socket.
     */
    public static Function<ReactiveSocket, ReactiveSocket> timeout(long timeout, TimeUnit unit, Scheduler scheduler) {
        return src -> ReactiveSocketDecorator.wrap(src)
                                             .decorateAllResponses(_timeout(timeout, unit, scheduler))
                                             .decorateAllVoidResponses(_timeout(timeout, unit, scheduler))
                                             .finish();
    }

    /**
     * Provides a mapping function to wrap a {@code ReactiveSocket} such that a call to {@link ReactiveSocket#close()}
     * does not cancel all pending requests. Instead, it will wait for all pending requests to finish and then close
     * the socket.
     *
     * @return Function to transform any socket into a safe closing socket.
     */
    public static Function<ReactiveSocket, ReactiveSocket> safeClose() {
        return src -> {
            final AtomicInteger count = new AtomicInteger();
            final AtomicBoolean closed = new AtomicBoolean();

            return ReactiveSocketDecorator.wrap(src)
                                          .close(reactiveSocket ->
                                              Px.defer(() -> {
                                                  if (closed.compareAndSet(false, true)) {
                                                      if (count.get() == 0) {
                                                          return src.close();
                                                      } else {
                                                          return src.onClose();
                                                      }
                                                  }
                                                  return src.onClose();
                                              })
                                          )
                                          .decorateAllResponses(_safeClose(src, closed, count))
                                          .decorateAllVoidResponses(_safeClose(src, closed, count))
                                          .finish();
        };
    }

    private static <T> Function<Publisher<T>, Publisher<T>> _timeout(long timeout, TimeUnit unit, Scheduler scheduler) {
        return t -> Px.from(t).timeout(timeout, unit, scheduler);
    }

    private static <T> Function<Publisher<T>, Publisher<T>> _safeClose(ReactiveSocket src, AtomicBoolean closed,
                                                                       AtomicInteger count) {
        return t -> Px.from(t)
                      .doOnSubscribe(s -> count.incrementAndGet())
                      .doOnTerminate(() -> {
                          if (count.decrementAndGet() == 0 && closed.get()) {
                              src.close().subscribe(Subscribers.empty());
                          }
                      });
    }
}
