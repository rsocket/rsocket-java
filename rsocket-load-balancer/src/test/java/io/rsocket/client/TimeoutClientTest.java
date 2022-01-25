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

package io.rsocket.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.client.filter.RSockets;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class TimeoutClientTest {
  @Test
  public void testTimeoutSocket() {
    TestingRSocket socket = new TestingRSocket((subscriber, payload) -> false);
    RSocket timeout = RSockets.timeout(Duration.ofMillis(50)).apply(socket);

    timeout
        .requestResponse(EmptyPayload.INSTANCE)
        .subscribe(
            new Subscriber<Payload>() {
              @Override
              public void onSubscribe(Subscription s) {
                s.request(1);
              }

              @Override
              public void onNext(Payload payload) {
                throw new AssertionError("onNext invoked when not expected.");
              }

              @Override
              public void onError(Throwable t) {
                assertThat(t)
                    .describedAs("Unexpected exception in onError")
                    .isInstanceOf(TimeoutException.class);
              }

              @Override
              public void onComplete() {
                throw new AssertionError("onComplete invoked when not expected.");
              }
            });
  }
}
