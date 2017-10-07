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

/** This class handles everything that gets printed to the console */
public class ConsoleUtils {

  private static final String ANSI_RESET = ""; // "\u001B[0m";
  private static final String ANSI_RED = ""; // \u001B[31m";
  private static final String ANSI_GREEN = ""; // \u001B[32m";
  private static final String ANSI_CYAN = ""; // \u001B[36m";
  private static final String ANSI_BLUE = ""; // \u001B[34m";
  private String agent;
  private final boolean debugEnabled = false;

  public ConsoleUtils(String s) {
    agent = s + " ";
  }

  /**
   * Logs something at the info level
   *
   * @param s message
   */
  public void info(String s) {
    if (debugEnabled) {
      System.out.println(agent + "INFO: " + s);
    }
  }

  /**
   * Logs a successful event
   *
   * @param s message
   */
  public void success(String s) {
    if (debugEnabled) {
      System.out.println(ANSI_GREEN + agent + "SUCCESS: " + s + ANSI_RESET);
    }
  }

  /**
   * Logs a time
   *
   * @param s message
   */
  public void time(String s) {
    if (debugEnabled) {
      System.out.println(ANSI_CYAN + agent + "TIME: " + s + ANSI_RESET);
    }
  }

  /**
   * Logs the initial payload the server has received
   *
   * @param s message
   */
  public void initialPayload(String s) {
    if (debugEnabled) {
      System.out.println(agent + s);
    }
  }

  /**
   * Logs the start of a test
   *
   * @param s message
   */
  public void teststart(String s) {
    if (debugEnabled) {
      System.out.println(ANSI_BLUE + agent + "TEST STARTING: " + s + ANSI_RESET);
    }
  }
}
