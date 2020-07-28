/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.stat;

/** This implementation gives better results because it considers more data-point. */
public class Median extends FrugalQuantile {
  public Median() {
    super(0.5, 1.0);
  }

  public synchronized void reset() {
    super.reset(0.5);
  }

  @Override
  public synchronized void insert(double x) {
    if (sign == 0) {
      estimate = x;
      sign = 1;
    } else {
      if (x > estimate) {
        greaterThanZero(x);
      } else if (x < estimate) {
        lessThanZero(x);
      }
    }
  }

  private void greaterThanZero(double x) {
    step += sign;

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
  }

  private void lessThanZero(double x) {
    step -= sign;

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
  }

  @Override
  public String toString() {
    return "Median(v=" + estimate + ")";
  }
}
