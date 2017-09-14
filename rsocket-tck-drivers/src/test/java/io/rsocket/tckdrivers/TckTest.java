package io.rsocket.tckdrivers;

import static java.util.stream.Collectors.toList;

import io.rsocket.Closeable;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.tckdrivers.client.JavaClientDriver;
import io.rsocket.tckdrivers.common.TckClientTest;
import io.rsocket.tckdrivers.common.TckTestSuite;
import io.rsocket.tckdrivers.server.JavaServerDriver;
import io.rsocket.transport.local.LocalServerTransport;
import java.io.File;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import reactor.core.publisher.Mono;

@RunWith(Parameterized.class)
public class TckTest {
  @Parameterized.Parameter(0)
  public TckTestSuite testSuite;

  @Parameterized.Parameter(1)
  public TckClientTest clientTest;

  /** Runs the test. */
  @Test(timeout = 10000)
  public void runTest() {
    JavaServerDriver d = testSuite.driver();

    LocalServerTransport st = LocalServerTransport.createEphemeral();
    Closeable server =
        RSocketFactory.receive().acceptor(d.acceptor()).transport(st).start().block();
    Mono<RSocket> client = RSocketFactory.connect().transport(st.clientTransport()).start();

    try {
      JavaClientDriver jd = new JavaClientDriver(client);
      jd.runTest(clientTest);
    } finally {
      server.close().block();
    }
  }

  /**
   * A function that reads all the server/client test files from "path". For each server file, it
   * starts a server. It parses each client file and create a parameterized test.
   */
  @Parameters(name = "{0} - {1}")
  public static Iterable<Object[]> data() {
    return TckTestSuite.loadAll(new File("src/test/resources"))
        .stream()
        .flatMap(s -> s.clientTests().stream().map(c -> new Object[] {s, c}))
        .collect(toList());
  }
}
