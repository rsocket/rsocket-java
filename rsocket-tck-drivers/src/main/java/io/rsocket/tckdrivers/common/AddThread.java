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

import java.util.concurrent.CountDownLatch;

/**
 * A thread that is created to wait to be able to append a marble string. We wait for the previous
 * thread to have finished adding before allowing this thread to append, and after adding, we call
 * countDown() to allow whatever thread waiting on this one to begin adding
 */
public class AddThread implements Runnable {

  private String marble;
  private ParseMarble parseMarble;
  private Thread t;
  private CountDownLatch prev, curr;

  public AddThread(
      String marble, ParseMarble parseMarble, CountDownLatch prev, CountDownLatch curr) {
    this.marble = marble;
    this.parseMarble = parseMarble;
    this.t = new Thread(this);
    this.prev = prev;
    this.curr = curr;
  }

  @Override
  public void run() {
    try {
      // await for the previous latch to have counted down, if it exists
      if (prev != null) prev.await();
      parseMarble.add(marble);
      curr.countDown(); // count down on the current to unblock the next append
    } catch (InterruptedException e) {
      System.out.println("Interrupted in AddThread");
    }
  }

  public void start() {
    t.start();
  }
}
