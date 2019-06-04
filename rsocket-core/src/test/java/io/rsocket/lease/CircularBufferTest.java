/*
 * Copyright 2015-2019 the original author or authors.
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

package io.rsocket.lease;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CircularBufferTest {
  private static final int MAX_SIZE = 3;

  private SlidingWindowLeaseStats.CircularBuffer<Integer> circularBuffer;

  @BeforeEach
  public void setUp() {
    circularBuffer = new SlidingWindowLeaseStats.CircularBuffer<>(MAX_SIZE);
  }

  @Test
  public void getNonExistentIndex() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> circularBuffer.get(0));
  }

  @Test
  public void offerNullElement() {
    org.junit.jupiter.api.Assertions.assertThrows(
        NullPointerException.class, () -> circularBuffer.offer(null));
  }

  @Test
  public void getNegativeIndex() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> circularBuffer.get(-1));
  }

  @Test
  public void emptyBufferSize() {
    Assertions.assertThat(circularBuffer.size()).isEqualTo(0);
    Assertions.assertThat(circularBuffer.maxSize()).isEqualTo(MAX_SIZE);
  }

  @Test
  public void singleElementBuffer() {
    circularBuffer = new SlidingWindowLeaseStats.CircularBuffer<>(1);
    circularBuffer.offer(0).offer(1).offer(2);
    Assertions.assertThat(circularBuffer.size()).isEqualTo(1);
    Assertions.assertThat(circularBuffer.get(0)).isEqualTo(2);
  }

  @Test
  public void multiElementBuffer() {
    circularBuffer.offer(0).offer(1).offer(2).offer(3).offer(4);
    Assertions.assertThat(circularBuffer.size()).isEqualTo(3);
    Assertions.assertThat(circularBuffer.get(0)).isEqualTo(4);
    Assertions.assertThat(circularBuffer.get(1)).isEqualTo(3);
    Assertions.assertThat(circularBuffer.get(2)).isEqualTo(2);
  }

  @Test
  public void clearBuffer() {
    circularBuffer.offer(0);
    circularBuffer.clear();
    Assertions.assertThat(circularBuffer.size()).isEqualTo(0);
  }

  @Test
  public void offerAfterClearBuffer() {
    circularBuffer.offer(0);
    circularBuffer.clear();
    circularBuffer.offer(0).offer(1).offer(2).offer(3).offer(4);
    Assertions.assertThat(circularBuffer.size()).isEqualTo(3);
    Assertions.assertThat(circularBuffer.get(0)).isEqualTo(4);
    Assertions.assertThat(circularBuffer.get(1)).isEqualTo(3);
    Assertions.assertThat(circularBuffer.get(2)).isEqualTo(2);
  }
}
