package io.rsocket.tckdrivers.runner;

import static io.rsocket.tckdrivers.runner.JsonUtil.parseTCKMessage;
import static io.rsocket.tckdrivers.runner.TckClient.connect;
import static io.rsocket.tckdrivers.runner.TckMessageUtil.printTestRunResults;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.rsocket.*;
import io.rsocket.util.PayloadImpl;
import java.util.Map;
import java.util.UUID;

@Command(name = "rsotcket-main", description = "This runs the TCM main")
public class SelfTestMain {

  @Option(name = "--url", description = "The server url")
  public String serverUrl = "tcp://localhost:30007";

  private RSocket socket;

  static SelfTestMain fromArgs(String... args) {
    return SingleCommand.singleCommand(SelfTestMain.class).parse(args);
  }

  public static void main(String... args) {
    fromArgs(args).run();
  }

  private void run() {
    System.out.println("Connecting to " + serverUrl);

    socket = connect(serverUrl, server -> new RunnerRSocket());

    System.out.println("Running self test");
    socket
        .requestStream(selfTest())
        .doOnNext(
            result -> {
              Map<String, Object> testRunResults = parseTCKMessage(result, "testSuiteResults");
              printTestRunResults(testRunResults);
            })
        .then()
        .block();
  }

  private Payload selfTest() {
    String uuid = UUID.randomUUID().toString();
    String version = "0.9-SNAPSHOT";
    return new PayloadImpl(
        "{\"selfTestRequest\":{\"runner\":{\"uuid\":\""
            + uuid
            + "\",\"codeversion\":\""
            + version
            + "\",\"capabilities\":{\"platform\":[\"rsocket-java\"],\"versions\":[\"1.0\"],"
            + "\"transports\":[\"tcp\",\"ws\",\"local\"],"
            + "\"modes\":[\"client\",\"server\"],"
            + "\"testFormats\":[\"tck1\"]}}}}");
  }
}
