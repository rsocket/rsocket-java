package io.rsocket.tckdrivers.runner;

import io.rsocket.Payload;
import io.rsocket.util.PayloadImpl;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class TckMessageUtil {
  public static String result(Map<String, Object> t) {
    String result = (String) t.get("result");
    String clientDetail = (String) t.get("clientDetail");
    String serverDetail = (String) t.get("serverDetail");

    if ("passed".equals(result)) {
      return "PASS";
    } else if (StringUtils.isNotEmpty(clientDetail)) {
      return clientDetail;
    } else if (StringUtils.isNotEmpty(serverDetail)) {
      return serverDetail;
    } else {
      return result;
    }
  }

  public static void printTestRunResults(Map<String, Object> testRunResults) {
    List<Map<String, Object>> suites = (List<Map<String, Object>>) testRunResults.get("suites");

    for (Map<String, Object> suite : suites) {
      Map<String, Object> setup = (Map<String, Object>) suite.get("setup");
      List<Map<String, Object>> tests = (List<Map<String, Object>>) suite.get("tests");

      for (Map<String, Object> t : tests) {
        String label =
            setup.get("version") + "\t" + setup.get("transport") + "\t" + t.get("testName");
        System.out.println(label + "\t" + TckMessageUtil.result(t));
      }
    }
  }

  public static Payload serverReady(String uri) {
    return new PayloadImpl("{\"runnerServerReady\":{\"url\":\"" + uri + "\"}}");
  }
}
