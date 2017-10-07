/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.tckdrivers.common;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.*;

import io.reactivex.subscribers.TestSubscriber;
import io.rsocket.Payload;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.*;

// TODO Remove RXJava2 TestSubscriber and replace with Reactor-core StepVerifier
public class MySubscriber<T> extends TestSubscriber<T> {

  private ConsoleUtils consoleUtils;

  private long maxAwait = 5000;
  /**
   * this will be locked everytime we await at most some number of myValues, the await will always
   * be with a timeout After the timeout, we look at the value inside the countdown latch to make
   * sure we counted down the number of myValues we expected
   */
  private CountDownLatch numOnNext = new CountDownLatch(Integer.MAX_VALUE);
  /** This latch handles the logic in take. */
  private CountDownLatch takeLatch = new CountDownLatch(Integer.MAX_VALUE);
  /** Keeps track if this test subscriber is myPassing */
  private boolean isComplete = false;

  private EchoSubscription echosub;

  private boolean isEcho = false;

  public MySubscriber(long initialRequest, String agent) {
    super(initialRequest);
    this.consoleUtils = new ConsoleUtils(agent);
  }

  @Override
  public void onSubscribe(Subscription s) {
    consoleUtils.info("MySubscriber: onSubscribe()");
    super.onSubscribe(s);
  }

  @Override
  public void onNext(T t) {
    Payload p = (Payload) t;
    Tuple<String, String> tup = new Tuple<>(p.getDataUtf8(), p.getMetadataUtf8());
    consoleUtils.info("On NEXT got : " + tup.getK() + " " + tup.getV());
    if (isEcho) {
      echosub.add(tup);
      return;
    }
    super.onNext(t);
    numOnNext.countDown();
    takeLatch.countDown();
  }

  public final void awaitAtLeast(long n) throws InterruptedException {
    int waitIterations = 0;
    while (valueCount() < n) {
      String msg = "Await at least timed out";
      long waited = (waitIterations * 100);
      assertThat(msg, waited, lessThan(maxAwait));

      numOnNext.await(100, TimeUnit.MILLISECONDS);
      waitIterations++;
    }
    myPass("Got " + valueCount() + " out of " + n + " values expected");
    numOnNext = new CountDownLatch(Integer.MAX_VALUE);
  }

  // could potentially have a race condition, but cancel is asynchronous anyways
  public final void awaitNoEvents(long time) throws InterruptedException {
    int nummyValues = values.size();
    boolean iscanceled = isCancelled();
    boolean iscompleted = isComplete;
    Thread.sleep(time);

    String msg = "Received additional events; ";
    assertEquals(msg, nummyValues, values.size());
    assertEquals(msg, iscanceled, isCancelled());
    assertEquals(msg, iscompleted, isComplete);

    myPass("No additional events");
  }

  public final void myAssertError(Throwable error) {
    String prefix = "";
    if (done.getCount() != 0) {
      prefix = "Subscriber still running! ";
    }

    String msg = prefix + "No errors; ";
    assertNotEquals(msg, 0, errors.size());
    myPass("Error received");
  }

  public final void assertValues(List<Tuple<String, String>> values) {
    String prefix = "";
    if (done.getCount() != 0) {
      prefix = "Subscriber still running! ";
    }

    assertReceivedAtLeast(values.size());

    for (int i = 0; i < values.size(); i++) {
      Payload p = (Payload) this.values().get(i);
      Tuple<String, String> v = new Tuple<>(p.getDataUtf8(), p.getMetadataUtf8());
      Tuple<String, String> u = values.get(i);
      String msg = prefix + "Values at position %d differ; ";
      assertEquals(String.format(msg, i), u, v);
    }
    myPass("All values match");
  }

  public final void assertValue(Tuple<String, String> value) {
    String prefix = "";
    if (done.getCount() != 0) {
      prefix = "Subscriber still running! ";
    }

    String msg = prefix;
    assertEquals(msg, 1, this.values.size());

    Payload p = (Payload) values().get(0);
    Tuple<String, String> v = new Tuple<>(p.getDataUtf8(), p.getMetadataUtf8());
    msg = prefix;
    assertEquals(msg, valueAndClass(value), valueAndClass(v));

    myPass("Value matches");
  }

  public void assertReceivedAtLeast(int count) {
    String prefix = "";
    if (done.getCount() != 0) {
      prefix = "Subscriber still running! ";
    }
    int s = values.size();
    String msg = prefix + "Received less; ";
    assertThat(msg, s, greaterThanOrEqualTo(count));

    myPass("Received " + s + " myValues");
  }

  private void myPass(String message) {
    consoleUtils.info("PASSED: " + message);
  }

  // there might be a race condition with take, so this behavior is defined as: either wait until we
  // have received n
  // myValues and then cancel, or cancel if we already have n myValues
  public final void take(long n) {
    if (values.size() >= n) {
      // if we've already received at least n myValues, then we cancel
      cancel();
      return;
    }
    int waitIterations = 0;
    while (Integer.MAX_VALUE - takeLatch.getCount() < n) {
      try {
        // we keep track of how long we've waited for
        if (waitIterations * 100 >= maxAwait) {
          fail("Timeout in take");
          break;
        }
        takeLatch.await(100, TimeUnit.MILLISECONDS);
        waitIterations++;
      } catch (Exception e) {
        assertNull("interrupted ", e.getMessage());
      }
    }
  }

  public Tuple<String, String> getElement(int n) {
    assert (n < values.size());
    Payload p = (Payload) values().get(n);
    return new Tuple<>(p.getDataUtf8(), p.getMetadataUtf8());
  }

  public final void setEcho(EchoSubscription echosub) {
    isEcho = true;
    this.echosub = echosub;
  }
}
