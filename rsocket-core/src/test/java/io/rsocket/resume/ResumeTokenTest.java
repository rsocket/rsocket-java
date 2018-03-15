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

package io.rsocket.resume;

import static org.junit.Assert.assertEquals;

import java.util.UUID;
import org.junit.Test;

public class ResumeTokenTest {
  @Test
  public void testFromUuid() {
    UUID x = UUID.fromString("3bac9870-3873-403a-99f4-9728aa8c7860");

    ResumeToken t = ResumeToken.bytes(ResumeToken.getBytesFromUUID(x));
    ResumeToken t2 = ResumeToken.bytes(ResumeToken.getBytesFromUUID(x));

    assertEquals("3bac98703873403a99f49728aa8c7860", t.toString());

    assertEquals(t.hashCode(), t2.hashCode());
    assertEquals(t, t2);
  }
}
