package io.rsocket.tckdrivers;

import static io.rsocket.tckdrivers.main.Main.port;
import static org.junit.Assert.assertNotNull;

import io.rsocket.Closeable;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.tckdrivers.client.JavaClientDriver;
import io.rsocket.tckdrivers.common.TckIndividualTest;
import java.io.File;
import java.util.*;

import io.rsocket.tckdrivers.server.JavaServerDriver;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.local.LocalServerTransport;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import reactor.core.publisher.Mono;

@RunWith(Parameterized.class)
public class TckTest {
  private TckIndividualTest tckTest;

  public TckTest(TckIndividualTest tckTest) {
    this.tckTest = tckTest;
  }

  /** Runs the test. */
  @Test(timeout = 10000)
  public void runTest() {
    JavaServerDriver d = tckTest.serverDriver();

    LocalServerTransport st = LocalServerTransport.createEphemeral();
    Closeable server = RSocketFactory.receive().acceptor(d.acceptor()).transport(st).start().block();
    Mono<RSocket> client =
        RSocketFactory.connect().transport(st.clientTransport()).start();
    try {
      JavaClientDriver jd = new JavaClientDriver(client);
      jd.runTest(this.tckTest.test.subList(1, this.tckTest.test.size()), this.tckTest.name);

    } finally {
      server.close().block();
    }
  }

  /**
   * A function that reads all the server/client test files from "path". For each server file, it
   * starts a server. It parses each client file and create a parameterized test.
   */
  @Parameters
  public static List<TckIndividualTest> data() throws Exception {
    return TckIndividualTest.list(new File("src/test/resources"));
  }
}
