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

package io.rsocket.test;

import io.rsocket.Payload;
import java.util.concurrent.CountDownLatch;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;

class CountdownBaseSubscriber extends BaseSubscriber<Payload> {
  private CountDownLatch latch = new CountDownLatch(0);
  private int count = 0;

  public void expect(int count) {
    latch = new CountDownLatch((int) latch.getCount() + count);
    if (upstream() != null) {
      request(count);
    }
  }

  @Override
  protected void hookOnNext(Payload value) {
    count++;
    latch.countDown();
  }

  @Override
  protected void hookOnSubscribe(Subscription subscription) {
    long count = latch.getCount();

    if (count > 0) {
      subscription.request(count);
    }
  }

  public void await() {
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public int count() {
    return count;
  }
}
