package io.rsocket.tckdrivers.main;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.rsocket.tckdrivers.client.JavaTCPClient;
import io.rsocket.tckdrivers.common.ConsoleUtils;
import io.rsocket.tckdrivers.common.ServerThread;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class fires up both the client and the server, is used for the Gradle task to run the tests
 */
@Command(
  name = "rsocket-test-driver",
  description = "This runs the client and servers that use the driver"
)
public class TestMain {

  @Option(name = "--debug", description = "set if you want frame level output")
  public static boolean debug;

  @Option(name = "--port", description = "The port")
  public static int port;

  @Option(
    name = "--serverfile",
    description = "The script file to parse, make sure to give the server the " + "correct file"
  )
  public static String serverfile;

  @Option(name = "--clientfile", description = "The script file for the client to parse")
  public static String clientfile;

  @Option(
    name = "--tests",
    description =
        "For the client only, optional argument to list out the tests you"
            + " want to run, should be comma separated names"
  )
  public static String tests;

  public static void main(String[] args) {
    SingleCommand<TestMain> cmd = SingleCommand.singleCommand(TestMain.class);
    cmd.parse(args);
    ServerThread st = new ServerThread(port, serverfile);
    st.start();
    st.awaitStart();
    try {
      if (tests != null)
        new JavaTCPClient()
            .run(clientfile, "localhost", port, debug, Arrays.asList(tests.split(",")));
      else new JavaTCPClient().run(clientfile, "localhost", port, debug, new ArrayList<>());
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (ConsoleUtils.allPassed()) {
      System.out.println("ALL TESTS PASSED");
      System.exit(0);
    } else {
      System.out.println("SOME TESTS FAILED");
      System.exit(1); // exit with code 1 so that the gradle build process fails
    }
  }
}
