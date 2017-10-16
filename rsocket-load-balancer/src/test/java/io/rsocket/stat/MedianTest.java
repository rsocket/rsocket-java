/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.stat;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class MedianTest {
  private double errorSum = 0;
  private double maxError = 0;
  private double minError = 0;

  @Test
  public void testMedian() {
    Random rng = new Random("Repeatable tests".hashCode());
    int n = 1;
    for (int i = 0; i < n; i++) {
      testMedian(rng);
    }
    System.out.println(
        "Error avg = " + (errorSum / n) + " in range [" + minError + ", " + maxError + "]");
  }

  /** Test Median estimation with normal random data */
  private void testMedian(Random rng) {
    int n = 100 * 1024;
    int range = Integer.MAX_VALUE >> 16;
    Median m = new Median();

    int[] data = new int[n];
    for (int i = 0; i < data.length; i++) {
      int x = Math.max(0, range / 2 + (int) (range / 5 * rng.nextGaussian()));
      data[i] = x;
      m.insert(x);
    }
    Arrays.sort(data);

    int expected = data[data.length / 2];
    double estimation = m.estimation();
    double error = Math.abs(expected - estimation) / expected;

    errorSum += error;
    maxError = Math.max(maxError, error);
    minError = Math.min(minError, error);

    assertTrue(
        error < 0.02, "p50=" + estimation + ", real=" + expected + ", error=" + error);
  }
}
