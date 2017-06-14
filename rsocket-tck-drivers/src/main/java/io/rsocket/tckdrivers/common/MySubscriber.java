
package io.rsocket.tckdrivers.common;

import io.reactivex.subscribers.TestSubscriber;
import io.rsocket.Payload;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.*;

// TODO Remove RXJava2 TestSubscriber and replace with Reactor-core StepVerifier
@Deprecated
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
  private boolean isPassing = true;

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
    Tuple<String, String> tup =
        new Tuple<>(
            StandardCharsets.UTF_8.decode(p.getData()).toString(),
            StandardCharsets.UTF_8.decode(p.getMetadata()).toString());
    consoleUtils.info("On NEXT got : " + tup.getK() + " " + tup.getV());
    if (isEcho) {
      echosub.add(tup);
      return;
    }
    super.onNext(t);
    numOnNext.countDown();
    takeLatch.countDown();
  }

  public final boolean awaitAtLeast(long n) throws InterruptedException {
    int waitIterations = 0;
    while (valueCount() < n) {
      if (waitIterations * 100 >= maxAwait) {
        myFail("Await at least timed out");
        break;
      }
      numOnNext.await(100, TimeUnit.MILLISECONDS);
      waitIterations++;
    }
    myPass("Got " + valueCount() + " out of " + n + " values expected");
    numOnNext = new CountDownLatch(Integer.MAX_VALUE);
    return true;
  }

  // could potentially have a race condition, but cancel is asynchronous anyways
  public final boolean awaitNoEvents(long time) throws InterruptedException {
    int nummyValues = values.size();
    boolean iscanceled = isCancelled();
    boolean iscompleted = isComplete;
    Thread.sleep(time);
    if (nummyValues == values.size() && iscanceled == isCancelled() && iscompleted == isComplete) {
      myPass("No additional events");
      return true;
    } else {
      myFail("Received additional events");
      return false;
    }
  }

  public final boolean myAssertError(Throwable error) {
    String prefix = "";
    if (done.getCount() != 0) {
      prefix = "Subscriber still running! ";
    }
    int s = errors.size();
    if (s == 0) {
      myFail(prefix + "No errors");
      return true;
    }
    myPass("Error received");
    return true;
  }

  public final boolean assertValues(List<Tuple<String, String>> values) {
    String prefix = "";
    if (done.getCount() != 0) {
      prefix = "Subscriber still running! ";
    }
    if (!assertReceivedAtLeast(values.size())) {
      return false;
    }
    for (int i = 0; i < values.size(); i++) {
      Payload p = (Payload) this.values().get(i);
      Tuple<String, String> v =
          new Tuple<>(
              StandardCharsets.UTF_8.decode(p.getData()).toString(),
              StandardCharsets.UTF_8.decode(p.getMetadata()).toString());
      Tuple<String, String> u = values.get(i);
      if (!Objects.equals(u, v)) {
        myFail(
            prefix
                + "Values at position "
                + i
                + " differ; Expected: "
                + valueAndClass(u)
                + ", Actual: "
                + valueAndClass(v));
        myFail("value does not match");
        return false;
      }
    }
    myPass("All values match");
    return true;
  }

  public final void assertValue(Tuple<String, String> value) {
    String prefix = "";
    if (done.getCount() != 0) {
      prefix = "Subscriber still running! ";
    }
    int s = this.values.size();
    if (s != 1) {
      myFail(prefix + "Expected: 1, Actual: " + valueCount());
      myFail("value does not match");
    }
    Payload p = (Payload) values().get(0);
    Tuple<String, String> v =
        new Tuple<>(
            StandardCharsets.UTF_8.decode(p.getData()).toString(),
            StandardCharsets.UTF_8.decode(p.getMetadata()).toString());
    if (!Objects.equals(value, v)) {
      myFail(prefix + "Expected: " + valueAndClass(value) + ", Actual: " + valueAndClass(v));
      myFail("value does not match");
    }
    myPass("Value matches");
  }

  public boolean assertReceivedAtLeast(int count) {
    String prefix = "";
    if (done.getCount() != 0) {
      prefix = "Subscriber still running! ";
    }
    int s = values.size();
    if (s < count) {
      myFail(prefix + "Received less; Expected at least: " + count + ", Actual: " + s);
      return false;
    }
    myPass("Received " + s + " myValues");
    return true;
  }

  private void myFail(String message) {
    isPassing = false;
    consoleUtils.info("FAILED: " + message);
  }

  private void myPass(String message) {
    consoleUtils.info("PASSED: " + message);
  }

  public boolean hasPassed() {
    return isPassing;
  }

  // there might be a race condition with take, so this behavior is defined as: either wait until we have received n
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
        consoleUtils.error("interrupted");
      }
    }
  }

  public Tuple<String, String> getElement(int n) {
    assert (n < values.size());
    Payload p = (Payload) values().get(n);
    Tuple<String, String> tup =
        new Tuple<>(
            StandardCharsets.UTF_8.decode(p.getData()).toString(),
            StandardCharsets.UTF_8.decode(p.getMetadata()).toString());
    return tup;
  }

  public final void setEcho(EchoSubscription echosub) {
    isEcho = true;
    this.echosub = echosub;
  }
}
