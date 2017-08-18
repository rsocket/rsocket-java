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

import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.Payload;
import io.rsocket.util.PayloadImpl;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import org.reactivestreams.Subscriber;

/**
 * This class parses through a marble diagram, but also implements a backpressure buffer so that the
 * rate at which producers append values can be much faster than the rate at which consumers consume
 * values. The backpressure buffer is the marble queue. The append function synchronously grows the
 * marble queue, and the request function synchronously increments the data requested as well as
 * unblocks the latches that are basically preventing the parse() method from emitting data in the
 * backpressure buffer that it should not.
 */
public class ParseMarble {

  private Queue<Character> marble;
  private Subscriber<? super Payload> s;
  private boolean cancelled = false;
  private Map<String, Map<String, String>> argMap;
  private long numSent = 0;
  private long numRequested = 0;
  private CountDownLatch parseLatch;
  private CountDownLatch sendLatch;
  private ConsoleUtils consoleUtils;

  /**
   * This constructor is useful if one already has the entire marble diagram before hand, so
   * append() does not need to be called.
   *
   * @param marble the whole marble diagram
   * @param s the subscriber
   * @param agent agent
   */
  public ParseMarble(String marble, Subscriber<? super Payload> s, String agent) {
    this.s = s;
    this.marble = new ConcurrentLinkedQueue<>();
    if (marble.contains("&&")) {
      String[] temp = marble.split("&&");
      marble = temp[0];
      ObjectMapper mapper = new ObjectMapper();
      try {
        argMap =
            mapper.readValue(temp[1], new TypeReference<Map<String, Map<String, String>>>() {});
      } catch (Exception e) {
        System.out.println("couldn't convert argmap");
      }
    }
    // we want to filter out and disregard '-' since we don't care about time
    for (char c : marble.toCharArray()) {
      if (c != '-') this.marble.add(c);
    }
    parseLatch = new CountDownLatch(1);
    sendLatch = new CountDownLatch(1);
    consoleUtils = new ConsoleUtils(agent);
  }

  /**
   * This constructor is useful for channel, when the marble diagram will be build incrementally.
   *
   * @param s the subscriber
   * @param agent agent
   */
  public ParseMarble(Subscriber<? super Payload> s, String agent) {
    this.s = s;
    this.marble = new ConcurrentLinkedQueue<>();
    parseLatch = new CountDownLatch(1);
    sendLatch = new CountDownLatch(1);
    consoleUtils = new ConsoleUtils(agent);
  }

  /**
   * This method is synchronized because we don't want two threads to try to append to the string at
   * once. Calling this method also unblocks the parseLatch, which allows non-emittable symbols to
   * be sent. In other words, it allows onNext and onComplete to be sent even if we've sent all the
   * values we've been requested of.
   *
   * @param m marble string
   */
  public synchronized void add(String m) {
    consoleUtils.info("adding " + m);
    for (char c : m.toCharArray()) {
      if (c != '-') this.marble.add(c);
    }
    if (!marble.isEmpty()) parseLatch.countDown();
  }

  /**
   * This method is synchronized because we only want to process one request at one time. Calling
   * this method unblocks the sendLatch as well as the parseLatch if we have more requests, as it
   * allows both emitted and non-emitted symbols to be sent,
   *
   * @param n credits to add
   */
  public synchronized void request(long n) {
    numRequested += n;
    if (!marble.isEmpty()) {
      parseLatch.countDown();
    }
    if (n > 0) sendLatch.countDown();
  }

  /** This function calls parse and executes the specified behavior in each line of commands */
  public void parse() {
    try {
      // if cancel has been called, don't do anything
      if (cancelled) return;
      while (true) {
        if (marble.isEmpty()) {
          synchronized (parseLatch) {
            if (parseLatch.getCount() == 0) parseLatch = new CountDownLatch(1);
            parseLatch.await();
          }
          parseLatch = new CountDownLatch(1);
        }
        char c = marble.poll();
        switch (c) {
          case '|':
            s.onComplete();
            consoleUtils.info("On complete sent");
            break;
          case '#':
            s.onError(new Throwable());
            consoleUtils.info("On error sent");
            break;
          default:
            if (numSent >= numRequested) {
              synchronized (sendLatch) {
                if (sendLatch.getCount() == 0) sendLatch = new CountDownLatch(1);
                sendLatch.await();
              }
              sendLatch = new CountDownLatch(1);
            }
            consoleUtils.info("numSent " + numSent + ": numRequested " + numRequested);
            if (argMap != null) {
              // this is hacky, but we only expect one key and one value
              Map<String, String> tempMap = argMap.get(c + "");
              if (tempMap == null) {
                s.onNext(new PayloadImpl(c + "", c + ""));
                consoleUtils.info("DATA SENT " + c + ", " + c);
              } else {
                List<String> key = new ArrayList<>(tempMap.keySet());
                List<String> value = new ArrayList<>(tempMap.values());
                s.onNext(new PayloadImpl(key.get(0), value.get(0)));
                consoleUtils.info("DATA SENT " + key.get(0) + ", " + value.get(0));
              }
            } else {
              this.s.onNext(new PayloadImpl(c + "", c + ""));
              consoleUtils.info("DATA SENT " + c + ", " + c);
            }

            numSent++;
            break;
        }
      }
    } catch (InterruptedException e) {
      assertNull("interrupted ", e.getMessage());
    }
  }

  /**
   * Since cancel is async, it just means that we will eventually, and rather quickly, stop emitting
   * values. We do this to follow the reactive streams specifications that cancel should mean that
   * the observable eventually stops emitting items.
   */
  public void cancel() {
    cancelled = true;
  }
}
