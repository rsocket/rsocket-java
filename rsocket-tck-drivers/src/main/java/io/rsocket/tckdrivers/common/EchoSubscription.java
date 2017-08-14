/*
 * Copyright 2016 Facebook, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.rsocket.tckdrivers.common;

import io.rsocket.Payload;
import io.rsocket.util.PayloadImpl;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * This class is a special subscription that allows us to implement echo tests without having to use
 * ay complex complex Rx constructs. Subscriptions handle the sending of data to whoever is
 * requesting it, this subscription has a very basic implementation of a backpressurebuffer that
 * allows for flow control, and allows that the rate at which elements are produced to it can differ
 * from the rate at which they are consumed.
 *
 * <p>This class should be passed inside of MySubscriber when one wants to do an echo test, so that
 * all the values that the MySubscriber receives immediately gets buffered here and prepared to be
 * sent. This implementation is needed because we want to send both the exact same data and
 * metadata. If we used our ParseMarble class, we could append a function to allow dynamic changing
 * of our argMap object, but even then, there are only small finite number of characters we can use
 * in the marble diagram.
 */
public class EchoSubscription implements Subscription {

  /** This is our backpressure buffer */
  private Queue<Tuple<String, String>> q;

  private long numSent = 0;
  private long numRequested = 0;
  private Subscriber<? super Payload> sub;
  private boolean cancelled = false;

  public EchoSubscription(Subscriber<? super Payload> sub) {
    q = new ConcurrentLinkedQueue<>();
    this.sub = sub;
  }

  /**
   * Every time our buffer grows, if there are still requests to satisfy, we need to send as much as
   * we can. We make this synchronized so we can avoid data races.
   *
   * @param payload the payload data and metadata
   */
  public void add(Tuple<String, String> payload) {
    q.add(payload);
    if (numSent < numRequested) request(0);
  }

  @Override
  public synchronized void request(long n) {
    numRequested += n;
    while (numSent < numRequested && !q.isEmpty() && !cancelled) {
      Tuple<String, String> tup = q.poll();
      System.out.println("Sending ... " + tup);
      sub.onNext(new PayloadImpl(tup.getK(), tup.getV()));
      numSent++;
    }
  }

  @Override
  public void cancel() {
    cancelled = true;
  }
}
