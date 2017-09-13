package io.rsocket.tckdrivers.runner;

import static io.rsocket.tckdrivers.runner.TckClient.connect;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.UUID;

@Command(name = "rsotcket-main", description = "This runs the TCM main")
public class RunnerMain {

  @Option(name = "--url", description = "The server url")
  public String serverUrl = "tcp://localhost:30007";

  static RunnerMain fromArgs(String... args) {
    return SingleCommand.singleCommand(RunnerMain.class).parse(args);
  }

  public static void main(String... args) {
    fromArgs(args).run();
  }

  private void run() {
    System.out.println("Connecting to " + serverUrl);

    RSocket socket = connect(serverUrl, server -> new RunnerRSocket());

    socket.requestResponse(registerPayload()).block();
    System.out.println("Registered");

    socket.onClose().block(Duration.ofDays(365));
  }

  private Payload registerPayload() {
    String uuid = UUID.randomUUID().toString();
    String version = "0.9-SNAPSHOT";
    return new PayloadImpl(
        "{\"registerRunner\":{\"uuid\":\""
            + uuid
            + "\",\"codeversion\":\""
            + version
            + "\","
            + "\"capabilities\":{\"platform\":[\"rsocket-java\"],\"versions\":[\"1.0\"],"
            + "\"transports\":[\"tcp\",\"ws\",\"local\"],"
            + "\"modes\":[\"client\",\"server\"],"
            + "\"testFormats\":[\"tck1\"]}}}");
  }
}
