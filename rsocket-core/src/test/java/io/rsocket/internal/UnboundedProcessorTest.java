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

package io.rsocket.internal;

import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.EmptyPayload;
import java.util.concurrent.CountDownLatch;
import org.junit.Assert;
import org.junit.Test;

public class UnboundedProcessorTest {
  @Test
  public void testOnNextBeforeSubscribe_10() {
    testOnNextBeforeSubscribeN(10);
  }

  @Test
  public void testOnNextBeforeSubscribe_100() {
    testOnNextBeforeSubscribeN(100);
  }

  @Test
  public void testOnNextBeforeSubscribe_10_000() {
    testOnNextBeforeSubscribeN(10_000);
  }

  @Test
  public void testOnNextBeforeSubscribe_100_000() {
    testOnNextBeforeSubscribeN(100_000);
  }

  @Test
  public void testOnNextBeforeSubscribe_1_000_000() {
    testOnNextBeforeSubscribeN(1_000_000);
  }

  @Test
  public void testOnNextBeforeSubscribe_10_000_000() {
    testOnNextBeforeSubscribeN(10_000_000);
  }

  public void testOnNextBeforeSubscribeN(int n) {
    UnboundedProcessor<Payload> processor = new UnboundedProcessor<>();

    for (int i = 0; i < n; i++) {
      processor.onNext(EmptyPayload.INSTANCE);
    }

    processor.onComplete();

    long count = processor.count().block();

    Assert.assertEquals(n, count);
  }

  @Test
  public void testOnNextAfterSubscribe_10() throws Exception {
    testOnNextAfterSubscribeN(10);
  }

  @Test
  public void testOnNextAfterSubscribe_100() throws Exception {
    testOnNextAfterSubscribeN(100);
  }

  @Test
  public void testOnNextAfterSubscribe_1000() throws Exception {
    testOnNextAfterSubscribeN(1000);
  }

  @Test
  public void testPrioritizedSending() {
    UnboundedProcessor<Payload> processor = new UnboundedProcessor<>();

    for (int i = 0; i < 1000; i++) {
      processor.onNext(EmptyPayload.INSTANCE);
    }

    processor.onNextPrioritized(ByteBufPayload.create("test"));

    Payload closestPayload = processor.next().block();

    Assert.assertEquals(closestPayload.getDataUtf8(), "test");
  }

  @Test
  public void testPrioritizedFused() {
    UnboundedProcessor<Payload> processor = new UnboundedProcessor<>();

    for (int i = 0; i < 1000; i++) {
      processor.onNext(EmptyPayload.INSTANCE);
    }

    processor.onNextPrioritized(ByteBufPayload.create("test"));

    Payload closestPayload = processor.poll();

    Assert.assertEquals(closestPayload.getDataUtf8(), "test");
  }

  public void testOnNextAfterSubscribeN(int n) throws Exception {
    CountDownLatch latch = new CountDownLatch(n);
    UnboundedProcessor<Payload> processor = new UnboundedProcessor<>();
    processor.log().doOnNext(integer -> latch.countDown()).subscribe();

    for (int i = 0; i < n; i++) {
      System.out.println("onNexting -> " + i);
      processor.onNext(EmptyPayload.INSTANCE);
    }

    processor.drain();

    latch.await();
  }
}
