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

package io.rsocket.frame;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class VersionCodecTest {
  @Test
  public void simple() {
    int version = VersionCodec.encode(1, 0);
    assertEquals(1, VersionCodec.major(version));
    assertEquals(0, VersionCodec.minor(version));
    assertEquals(0x00010000, version);
    assertEquals("1.0", VersionCodec.toString(version));
  }

  @Test
  public void complex() {
    int version = VersionCodec.encode(0x1234, 0x5678);
    assertEquals(0x1234, VersionCodec.major(version));
    assertEquals(0x5678, VersionCodec.minor(version));
    assertEquals(0x12345678, version);
    assertEquals("4660.22136", VersionCodec.toString(version));
  }

  @Test
  public void noShortOverflow() {
    int version = VersionCodec.encode(43210, 43211);
    assertEquals(43210, VersionCodec.major(version));
    assertEquals(43211, VersionCodec.minor(version));
  }
}
