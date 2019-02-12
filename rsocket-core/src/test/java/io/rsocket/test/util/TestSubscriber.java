/*
 * Copyright 2015-2018 the original author or authors.
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
 */

package io.rsocket.test.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class TestSubscriber {
  public static <T> Subscriber<T> create() {
    return create(Long.MAX_VALUE);
  }

  public static <T> Subscriber<T> create(long initialRequest) {
    @SuppressWarnings("unchecked")
    Subscriber<T> mock = mock(Subscriber.class);

    Mockito.doAnswer(
            invocation -> {
              if (initialRequest > 0) {
                ((Subscription) invocation.getArguments()[0]).request(initialRequest);
              }
              return null;
            })
        .when(mock)
        .onSubscribe(any(Subscription.class));

    return mock;
  }

  public static Payload anyPayload() {
    return any(Payload.class);
  }

  public static Subscriber<Payload> createCancelling() {
    @SuppressWarnings("unchecked")
    Subscriber<Payload> mock = mock(Subscriber.class);

    Mockito.doAnswer(
            invocation -> {
              ((Subscription) invocation.getArguments()[0]).cancel();
              return null;
            })
        .when(mock)
        .onSubscribe(any(Subscription.class));

    return mock;
  }
}
