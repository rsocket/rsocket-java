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

import static java.util.stream.Collectors.toList;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TckClientTest {
  public String name;
  public List<String> test;

  public TckClientTest(String name, List<String> test) {
    this.name = name;
    this.test = test;
  }

  public List<String> testLines() {
    return test;
  }

  public static List<TckClientTest> extractTests(File file) {
    try {
      return split(Files.readLines(file, StandardCharsets.UTF_8))
          .stream()
          .map(testLines -> new TckClientTest(parseName(testLines.remove(0)), testLines))
          .collect(toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<List<String>> split(List<String> lines) {
    List<List<String>> testLines = new ArrayList<>();

    List<String> test = new ArrayList<>();

    for (String line : lines) {
      switch (line) {
        case "!":
          if (!test.isEmpty()) {
            testLines.add(test);
            test = new ArrayList<>();
          }
          break;
        default:
          test.add(line);
      }
    }

    return testLines;
  }

  private static String parseName(String nameLine) {
    return nameLine.split("%%")[1];
  }

  @Override
  public String toString() {
    return name;
  }
}
