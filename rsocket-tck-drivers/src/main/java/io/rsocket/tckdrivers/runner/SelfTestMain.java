package io.rsocket.tckdrivers.runner;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.rsocket.*;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.uri.UriTransportRegistry;
import io.rsocket.util.PayloadImpl;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.rsocket.tckdrivers.runner.JsonUtil.parseTCKMessage;
import static io.rsocket.tckdrivers.runner.TckClient.connect;

@Command(name = "rsotcket-main", description = "This runs the TCM main")
public class SelfTestMain {

    @Option(name = "--url", description = "The server url")
    public String serverUrl = "tcp://localhost:30007";

    private RSocket socket;
    private AtomicInteger localCounter = new AtomicInteger();

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

        for (Map<String, Object> t: tests) {
            System.out.println(t.get("testName") + "\t" + t.get("result"));
        }
    }

    private Payload selfTest() {
        String uuid = UUID.randomUUID().toString();
        String version = "0.9-SNAPSHOT";
        return new PayloadImpl("{\"selfTestRequest\":{\"runner\":{\"uuid\":\"" + uuid +"\",\"codeversion\":\"" + version + "\",\"capabilities\":{\"platform\":[\"rsocket-java\"],\"versions\":[\"1.0\"],\"transports\":[\"http\",\"tcp\",\"ws\",\"local\"],\"modes\":[\"client\",\"server\"],\"testFormats\":[\"tck1\"]}}}}");
    }

    private RSocket createRequestHandler(RSocket serverConnection) {
        return new AbstractRSocket() {
            @Override
            public Mono<Payload> requestResponse(Payload payload) {
                Map<String, Object> clientTest = parseTCKMessage(payload, "runnerExecuteTest");

                System.out.println("clientTest " + clientTest);

                return Mono.just(new PayloadImpl("{\"runnerTestResults\":{\"result\":{\"testName\":\"test\",\"result\":\"passed\"}}}"));
            }

            @Override
            public Flux<Payload> requestStream(Payload payload) {
                Map<String, Object> serverTest = parseTCKMessage(payload, "runnerExecuteTest");

                System.out.println("serverTest " + serverTest);
                Map<String, Object> setup = (Map<String, Object>) serverTest.get("setup");
                Map<String, Object> test = (Map<String, Object>) serverTest.get("test");
                // TODO check version
                String version = (String) setup.get("version");
                String transport = (String) setup.get("transport");
                String testName = "test";
                String serverScript = "serverScript";

                String uri = urlForTransport(transport);
                Mono<Closeable> server = RSocketFactory.receive().acceptor((s, rs) -> testRunner(testName, serverScript))
                        .transport(UriTransportRegistry.serverForUri(uri)).start();

                return server.flatMapMany(closeable -> Flux.just(serverReady(transport, uri, closeable)).concatWith(Flux.never()).doFinally(s -> {
                    closeable.close();
                })).doOnError(e -> e.printStackTrace());
            }
        };
    }

    private Payload serverReady(String transport, String uri, Closeable closeable) {
        if (transport.equals("tcp")) {
            // TODO get external IP?
            uri = "tcp://localhost:" + nettyPort(closeable);
        } else if (transport.equals("ws")) {
            // TODO get external IP?
            uri = "ws://localhost:" + nettyPort(closeable);
        }

        return new PayloadImpl("{\"runnerServerReady\":{\"url\":\"" + uri + "\"}}");
    }

    private int nettyPort(Closeable closeable) {
        return ((NettyContextCloseable) closeable).address().getPort();
    }

    private String urlForTransport(String transport) {
        if (transport.equals("local")) {
            return "local:test" + localCounter.incrementAndGet();
        } else if (transport.equals("tcp")) {
            // TODO get external IP?
            return "tcp://localhost:0";
        } else if (transport.equals("ws")) {
            // TODO get external IP?
            return "ws://localhost:0";
        } else {
            throw new UnsupportedOperationException("unknown transport '" + transport + "'");
        }
    }

    private Mono<RSocket> testRunner(String testName, String serverScript) {
        // TODO
        return Mono.just(new AbstractRSocket() {
        });
    }
}
