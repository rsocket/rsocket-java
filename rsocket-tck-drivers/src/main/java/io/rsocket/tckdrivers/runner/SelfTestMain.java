package io.rsocket.tckdrivers.runner;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.rsocket.*;
import io.rsocket.tckdrivers.client.JavaClientDriver;
import io.rsocket.tckdrivers.common.TckClientTest;
import io.rsocket.tckdrivers.common.TckTestSuite;
import io.rsocket.tckdrivers.server.JavaServerDriver;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.uri.UriTransportRegistry;
import io.rsocket.util.PayloadImpl;
import java.util.Arrays;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.rsocket.tckdrivers.runner.JsonUtil.parseTCKMessage;
import static io.rsocket.tckdrivers.runner.TckClient.connect;
import static io.rsocket.tckdrivers.runner.Transports.actualLocalUrl;
import static io.rsocket.tckdrivers.runner.Transports.urlForTransport;
import static java.util.Arrays.asList;

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

    socket = connect(serverUrl, this::createRequestHandler);

    System.out.println("Running self test");
    Payload result = socket.requestResponse(selfTest()).block();

    Map<String, Object> testRunResults = parseTCKMessage(result, "testRunResults");
    List<Map<String, Object>> suites = (List<Map<String, Object>>) testRunResults.get("suites");
    Map<String, Object> suite0 = suites.get(0);
    List<Map<String, Object>> tests = (List<Map<String, Object>>) suite0.get("tests");

    for (Map<String, Object> t : tests) {
      System.out.println(
          t.get("testName")
              + "\t"
              + t.get("result")
              + "\t"
              + t.get("clientDetail")
              + "\t"
              + t.get("serverDetail"));
    }
  }

  private Payload selfTest() {
    String uuid = UUID.randomUUID().toString();
    String version = "0.9-SNAPSHOT";
    return new PayloadImpl(
        "{\"selfTestRequest\":{\"runner\":{\"uuid\":\""
            + uuid
            + "\",\"codeversion\":\""
            + version
            + "\",\"capabilities\":{\"platform\":[\"rsocket-java\"],\"versions\":[\"1.0\"],\"transports\":[\"http\",\"tcp\",\"ws\",\"local\"],\"modes\":[\"client\",\"server\"],\"testFormats\":[\"tck1\"]}}}}");
  }

  private RSocket createRequestHandler(RSocket serverConnection) {
    return new AbstractRSocket() {
      @Override
      public Mono<Payload> requestResponse(Payload payload) {
        Map<String, Object> clientTest = parseTCKMessage(payload, "runnerExecuteTest");
        Map<String, Object> setup = (Map<String, Object>) clientTest.get("setup");
        Map<String, Object> test = (Map<String, Object>) clientTest.get("test");
        Map<String, Object> tck1Definition = (Map<String, Object>) test.get("tck1Definition");

        String url = (String) setup.get("url");
        String testName = (String) test.get("testName");
        String clientScript = (String) tck1Definition.get("clientScript");

        Mono<RSocket> client =
            RSocketFactory.connect().transport(UriTransportRegistry.clientForUri(url)).start();

        try {
          JavaClientDriver jd2 = new JavaClientDriver(client);

          // TODO run in another thread
          jd2.runTest(new TckClientTest(testName, asList(clientScript.split("\n"))));

          return Mono.just(
              new PayloadImpl(
                  "{\"runnerTestResults\":{\"result\":{\"testName\":\""
                      + testName
                      + "\",\"result\":\"passed\"}}}"));
        } catch (Exception e) {
          return Mono.just(
              new PayloadImpl(
                  "{\"runnerTestResults\":{\"result\":{\"testName\":\""
                      + testName
                      + "\",\"result\":\"failed\",\"clientDetail\":\""
                      + e.toString()
                      + "\"}}}"));
        }
      }

      @Override
      public Flux<Payload> requestStream(Payload payload) {
        Map<String, Object> serverTest = parseTCKMessage(payload, "runnerExecuteTest");

        Map<String, Object> setup = (Map<String, Object>) serverTest.get("setup");
        Map<String, Object> test = (Map<String, Object>) serverTest.get("test");
        Map<String, Object> tck1Definition = (Map<String, Object>) test.get("tck1Definition");

        // TODO check version
        String version = (String) setup.get("version");
        String transport = (String) setup.get("transport");
        String testName = (String) test.get("testName");
        String serverScript = (String) tck1Definition.get("serverScript");

        JavaServerDriver javaServerDriver = new JavaServerDriver();
        javaServerDriver.parse(asList(serverScript.split("\n")));

        String uri = urlForTransport(transport);
        Mono<Closeable> server =
            RSocketFactory.receive()
                .acceptor(javaServerDriver.acceptor())
                .transport(UriTransportRegistry.serverForUri(uri))
                .start();

        return server
            .flatMapMany(
                closeable -> {
                  String actualUri = actualLocalUrl(transport, uri, closeable);
                  return Flux.just(serverReady(actualUri, closeable))
                      .concatWith(Flux.never())
                      .doFinally(s -> closeable.close());
                })
            .doOnError(e -> e.printStackTrace());
      }
    };
  }

  private Payload serverReady(String uri, Closeable closeable) {

    return new PayloadImpl("{\"runnerServerReady\":{\"url\":\"" + uri + "\"}}");
  }
}
