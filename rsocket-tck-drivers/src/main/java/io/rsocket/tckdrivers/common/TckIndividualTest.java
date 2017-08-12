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

import io.rsocket.tckdrivers.server.JavaServerDriver;

import java.io.File;
import java.util.List;

public class TckIndividualTest {
  public String name; // Test name
  public List<String> test; // test instructions/commands
  public String testFile; // Test belong to this file. File name is without client/server prefix
  public static final String serverPrefix = "server";
  public static final String clientPrefix = "client";

  public TckIndividualTest(String name, List<String> test, String testFile) {
    this.name = name;
    this.test = test;
    this.testFile = testFile;
  }

  public List<String> testLines() {
    return test.subList(1, test.size());
  }

  public static List<TckIndividualTest> list(File file) {
    File[] listOfFiles = folder.listFiles(f -> f.getName().startsWith("client"));

    for (int i = 0; i < listOfFiles.length; i++) {
      File file = listOfFiles[i];
      if (file.getName().startsWith(TckIndividualTest.clientPrefix)) {
        for (TckIndividualTest t : ) {
          Object testObject[] = new Object[2];
          testObject[0] = t.name + " (" + file.getName() + ")";
          testObject[1] = t;
          testData.add(testObject);
        }
      }
    }

  }

  public JavaServerDriver serverDriver() {
  }
}
