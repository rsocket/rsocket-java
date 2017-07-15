package io.rsocket.util;

public class ExceptionUtil {
  public static <T extends Throwable> T noStacktrace(T ex) {
    ex.setStackTrace(
        new StackTraceElement[] {
          new StackTraceElement(ex.getClass().getName(), "<init>", null, -1)
        });
    return ex;
  }
}
