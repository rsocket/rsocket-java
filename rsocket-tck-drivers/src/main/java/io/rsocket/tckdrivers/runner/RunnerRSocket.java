package io.rsocket.tckdrivers.runner;

import static io.rsocket.tckdrivers.runner.JsonUtil.parseTCKMessage;
import static io.rsocket.tckdrivers.runner.TckMessageUtil.serverReady;
import static io.rsocket.tckdrivers.runner.Transports.actualLocalUrl;
import static io.rsocket.tckdrivers.runner.Transports.urlForTransport;
import static java.util.Arrays.asList;

import io.rsocket.AbstractRSocket;
import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.tckdrivers.client.JavaClientDriver;
import io.rsocket.tckdrivers.common.TckClientTest;
import io.rsocket.tckdrivers.server.JavaServerDriver;
import io.rsocket.uri.UriTransportRegistry;
import io.rsocket.util.PayloadImpl;
import java.util.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

class RunnerRSocket extends AbstractRSocket {
  public RunnerRSocket() {}

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    Map<String, Object> clientTest = parseTCKMessage(payload, "runnerExecuteTest");
    @SuppressWarnings("unchecked")
    Map<String, Object> setup = (Map<String, Object>) clientTest.get("setup");
    @SuppressWarnings("unchecked")
    Map<String, Object> test = (Map<String, Object>) clientTest.get("test");
    @SuppressWarnings("unchecked")
    Map<String, Object> tck1Definition = (Map<String, Object>) test.get("tck1Definition");

    String url = (String) setup.get("url");
    String testName = (String) test.get("testName");
    String clientScript = (String) tck1Definition.get("clientScript");

    Mono<RSocket> client =
        RSocketFactory.connect().transport(UriTransportRegistry.clientForUri(url)).start();

    JavaClientDriver jd2 = new JavaClientDriver(client);

    Mono<Payload> mono =
        Mono.create(
            sink -> {
              try {
                jd2.runTest(new TckClientTest(testName, asList(clientScript.split("\n"))));

                sink.success(
                    new PayloadImpl(
                        "{\"runnerTestResult\":{\"result\":{\"testName\":\""
                            + testName
                            + "\",\"result\":\"passed\"}}}"));
              } catch (Exception e) {
                sink.success(
                    new PayloadImpl(
                        "{\"runnerTestResult\":{\"result\":{\"testName\":\""
                            + testName
                            + "\",\"result\":\"failed\",\"clientDetail\":\""
                            + e.toString()
                            + "\"}}}"));
              }
            });
    return mono.subscribeOn(Schedulers.elastic());
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    Map<String, Object> serverTest = parseTCKMessage(payload, "runnerExecuteTest");

    @SuppressWarnings("unchecked")
    Map<String, Object> setup = (Map<String, Object>) serverTest.get("setup");
    @SuppressWarnings("unchecked")
    Map<String, Object> test = (Map<String, Object>) serverTest.get("test");
    @SuppressWarnings("unchecked")
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
              return Flux.just(serverReady(actualUri))
                  .concatWith(Flux.never())
                  .doFinally(s -> closeable.close());
            })
        .doOnError(Throwable::printStackTrace);
  }
}
