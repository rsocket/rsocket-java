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
        "java.lang.RuntimeException: RE\n"
            + "\tat java.lang.RuntimeException.<init>(Unknown Source)\n",
        stacktraceString(ex));
  }

  private String stacktraceString(RuntimeException ex) {
    StringWriter stringWriter = new StringWriter();
    ex.printStackTrace(new PrintWriter(stringWriter));
    return stringWriter.toString();
  }
}
