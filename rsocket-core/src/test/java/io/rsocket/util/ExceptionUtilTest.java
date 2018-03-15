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

package io.rsocket.util;

import static io.rsocket.util.ExceptionUtil.noStacktrace;
import static org.junit.Assert.assertEquals;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.Test;

public class ExceptionUtilTest {
  @Test
  public void testNoStacktrace() {
    RuntimeException ex = noStacktrace(new RuntimeException("RE"));
    assertEquals(
        String.format(
            "java.lang.RuntimeException: RE%n"
                + "\tat java.lang.RuntimeException.<init>(Unknown Source)%n"),
        stacktraceString(ex));
  }

  private String stacktraceString(RuntimeException ex) {
    StringWriter stringWriter = new StringWriter();
    ex.printStackTrace(new PrintWriter(stringWriter));
    return stringWriter.toString();
  }
}
