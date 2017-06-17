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

  private static final String ANSI_RESET = ""; //"\u001B[0m";
  private static final String ANSI_RED = ""; //\u001B[31m";
  private static final String ANSI_GREEN = ""; //\u001B[32m";
  private static final String ANSI_CYAN = ""; //\u001B[36m";
  private static final String ANSI_BLUE = ""; //\u001B[34m";
  private static boolean allPassed = true;
  private String agent;

  public ConsoleUtils(String s) {
    agent = s + " ";
  }

  /** Logs something at the info level */
  public void info(String s) {
    System.out.println(agent + "INFO: " + s);
  }

  /** Logs a successful event */
  public void success(String s) {
    System.out.println(ANSI_GREEN + agent + "SUCCESS: " + s + ANSI_RESET);
  }

  /**
   * Logs a failure event, and sets the allPassed boolean in this class to false. This can be used
   * to check if there have been any failures in any tests after all tests have been run by the
   * driver.
   */
  public void failure(String s) {
    allPassed = false;
    System.out.println(ANSI_RED + agent + "FAILURE: " + s + ANSI_RESET);
  }

  /**
   * Logs an error event, and sets the allPassed boolean in this class to false. This can be used to
   * check if there have been any failures in any tests after all tests have been run by the driver.
   */
  public void error(String s) {
    allPassed = false;
    System.out.println(agent + "ERROR: " + s);
  }

  /** Logs a time */
  public void time(String s) {
    System.out.println(ANSI_CYAN + agent + "TIME: " + s + ANSI_RESET);
  }

  /** Logs the initial payload the server has received */
  public void initialPayload(String s) {
    System.out.println(agent + s);
  }

  /** Logs the start of a test */
  public void teststart(String s) {
    System.out.println(ANSI_BLUE + agent + "TEST STARTING: " + s + ANSI_RESET);
  }

  /**
   * Returns whether or not all tests up to the point this method is called, has passed
   *
   * @return false if there has been any failure or error, true if everything has passed
   */
  public static boolean allPassed() {
    return allPassed;
  }
}
