/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.loadbalance;

import java.util.SplittableRandom;

/**
 * Reference: Ma, Qiang, S. Muthukrishnan, and Mark Sandler. "Frugal Streaming for Estimating
 * Quantiles." Space-Efficient Data Structures, Streams, and Algorithms. Springer Berlin Heidelberg,
 * 2013. 77-96.
 *
 * <p>More info: http://blog.aggregateknowledge.com/2013/09/16/sketch-of-the-day-frugal-streaming/
 */
class FrugalQuantile implements Quantile {
  final double increment;
  final SplittableRandom rnd;

  int step;
  int sign;
  double quantile;

  volatile double estimate;

  public FrugalQuantile(double quantile, double increment) {
    this.increment = increment;
    this.quantile = quantile;
    this.estimate = 0.0;
    this.step = 1;
    this.sign = 0;
    this.rnd = new SplittableRandom(System.nanoTime());
  }

  public FrugalQuantile(double quantile) {
    this(quantile, 1.0);
  }

  public synchronized void reset(double quantile) {
    this.quantile = quantile;
    this.estimate = 0.0;
    this.step = 1;
    this.sign = 0;
  }

  public double estimation() {
    return estimate;
  }

  @Override
  public synchronized void insert(double x) {
    if (sign == 0) {
      estimate = x;
      sign = 1;
    } else {
      final double v = rnd.nextDouble();
      final double estimate = this.estimate;

      if (x > estimate && v > (1 - quantile)) {
        higher(x);
      } else if (x < estimate && v > quantile) {
        lower(x);
      }
    }
  }

  private void higher(double x) {
    double estimate = this.estimate;

    step += sign * increment;

    if (step > 0) {
      estimate += step;
    } else {
      estimate += 1;
    }

    if (estimate > x) {
      step += (x - estimate);
      estimate = x;
    }

    if (sign < 0) {
      step = 1;
    }

    sign = 1;

    this.estimate = estimate;
  }

  private void lower(double x) {
    double estimate = this.estimate;

    step -= sign * increment;

    if (step > 0) {
      estimate -= step;
    } else {
      estimate--;
    }

    if (estimate < x) {
      step += (estimate - x);
      estimate = x;
    }

    if (sign > 0) {
      step = 1;
    }

    sign = -1;

    this.estimate = estimate;
  }

  @Override
  public String toString() {
    return "FrugalQuantile(q=" + quantile + ", v=" + estimate + ")";
  }
}
