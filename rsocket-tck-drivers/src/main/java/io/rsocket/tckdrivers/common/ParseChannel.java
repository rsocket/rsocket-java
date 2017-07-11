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

import io.rsocket.Payload;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/** This class is exclusively used to parse channel commands on both the client and the server */
public class ParseChannel {

  private List<String> commands;
  private MySubscriber<Payload> sub;
  private ParseMarble parseMarble;
  private String name = "";
  private CountDownLatch prevRespondLatch;
  private CountDownLatch currentRespondLatch;
  public ConsoleUtils consoleUtils;

  public ParseChannel(
      List<String> commands, MySubscriber<Payload> sub, ParseMarble parseMarble, String agent) {
    this.commands = commands;
    this.sub = sub;
    this.parseMarble = parseMarble;
    ParseThread parseThread = new ParseThread(parseMarble);
    parseThread.start();
    consoleUtils = new ConsoleUtils(agent);
  }

  public ParseChannel(
      List<String> commands,
      MySubscriber<Payload> sub,
      ParseMarble parseMarble,
      String name,
      boolean pass,
      String agent) {
    this.commands = commands;
    this.sub = sub;
    this.parseMarble = parseMarble;
    this.name = name;
    ParseThread parseThread = new ParseThread(parseMarble);
    parseThread.start();
    consoleUtils = new ConsoleUtils(agent);
  }

  /**
   * This parses through each line of the marble test and executes the commands in each line Most of
   * the functionality is the same as the switch statement in the JavaClientDriver, but this also
   * allows for the channel to stage items to emit.
   */
  public void parse() {
    for (String line : commands) {
      String[] args = line.split("%%");
      switch (args[0]) {
        case "respond":
          handleResponse(args);
          break;
        case "await":
          switch (args[1]) {
            case "terminal":
              sub.awaitTerminalEvent();
              break;
            case "atLeast":
              try {
                sub.awaitAtLeast(Long.parseLong(args[3]));
              } catch (InterruptedException e) {
                assertNull("interrupted ", e.getMessage());
              }
              break;
            case "no_events":
              try {
                sub.awaitNoEvents(Long.parseLong(args[3]));
              } catch (InterruptedException e) {
                assertNull("interrupted ", e.getMessage());
              }
              break;
          }
          break;
        case "assert":
          switch (args[1]) {
            case "no_error":
              sub.assertNoErrors();
              break;
            case "error":
              sub.assertError(new Throwable());
              break;
            case "received":
              handleReceived(args);
              break;
            case "received_n":
              sub.assertValueCount(Integer.parseInt(args[3]));
              break;
            case "received_at_least":
              sub.assertReceivedAtLeast(Integer.parseInt(args[3]));
              break;
            case "completed":
              sub.assertComplete();
              break;
            case "no_completed":
              sub.assertNotComplete();
              break;
            case "canceled":
              sub.isCancelled();
              break;
          }
          break;
        case "take":
          sub.take(Long.parseLong(args[1]));
          break;
        case "request":
          sub.request(Long.parseLong(args[1]));
          consoleUtils.info("requesting " + args[1]);
          break;
        case "cancel":
          sub.cancel();
          break;
      }
    }
    if (name.equals("")) {
      name = "CHANNEL";
    }
    consoleUtils.success(name);
  }

  /**
   * On handling a command to respond with something, we create an AddThread and pass in latches to
   * make sure that we don't let this thread request to append something before the previous thread
   * has added something.
   *
   * @param args
   */
  private void handleResponse(String[] args) {
    consoleUtils.info("responding " + args);
    if (currentRespondLatch == null) currentRespondLatch = new CountDownLatch(1);
    AddThread addThread =
        new AddThread(args[1], parseMarble, prevRespondLatch, currentRespondLatch);
    prevRespondLatch = currentRespondLatch;
    currentRespondLatch = new CountDownLatch(1);
    addThread.start();
  }

  /**
   * This verifies that the data received by our MySubscriber matches what we expected
   *
   * @param args
   */
  private void handleReceived(String[] args) {
    String[] values = args[3].split("&&");
    if (values.length == 1) {
      String[] temp = values[0].split(",");
      sub.assertValue(new Tuple<>(temp[0], temp[1]));
    } else if (values.length > 1) {
      List<Tuple<String, String>> assertList = new ArrayList<>();
      for (String v : values) {
        String[] vals = v.split(",");
        assertList.add(new Tuple<>(vals[0], vals[1]));
      }
      sub.assertValues(assertList);
    }
  }
}
