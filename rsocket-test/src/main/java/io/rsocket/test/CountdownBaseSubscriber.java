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
