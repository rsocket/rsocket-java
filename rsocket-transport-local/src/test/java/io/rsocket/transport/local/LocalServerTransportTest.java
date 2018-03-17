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

package io.rsocket.transport.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

public class LocalServerTransportTest {
  @Test
  public void testEphemeral() {
    LocalServerTransport st1 = LocalServerTransport.createEphemeral();
    LocalServerTransport st2 = LocalServerTransport.createEphemeral();
    assertNotEquals(st2.getName(), st1.getName());
  }

  @Test
  public void testNamed() {
    LocalServerTransport st = LocalServerTransport.create("LocalServerTransportTest");
    assertEquals("LocalServerTransportTest", st.getName());
  }
}
