package io.rsocket.internal;

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
    UnboundedProcessor<Integer> processor = new UnboundedProcessor<>();

    for (int i = 0; i < n; i++) {
      processor.onNext(i);
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

  public void testOnNextAfterSubscribeN(int n) throws Exception {
    CountDownLatch latch = new CountDownLatch(n);
    UnboundedProcessor<Integer> processor = new UnboundedProcessor<>();
    processor.log().doOnNext(integer -> latch.countDown()).subscribe();

    for (int i = 0; i < n; i++) {
      System.out.println("onNexting -> " + i);
      processor.onNext(i);
    }

    processor.drain();

    latch.await();
  }
}
