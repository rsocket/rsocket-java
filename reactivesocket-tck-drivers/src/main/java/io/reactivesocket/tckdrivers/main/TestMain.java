package io.reactivesocket.tckdrivers.main;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.reactivesocket.tckdrivers.client.JavaTCPClient;
import io.reactivesocket.tckdrivers.common.*;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class fires up both the client and the server, is used for the Gradle task to run the tests
 */
@Command(name = "reactivesocket-test-driver", description = "This runs the client and servers that use the driver")
public class TestMain {

    @Option(name = "--debug", description = "set if you want frame level output")
    public static boolean debug;

    @Option(name = "--port", description = "The port")
    public static int port;

    @Option(name = "--serverfile", description = "The script file to parse, make sure to give the server the " +
            "correct file")
    public static String serverfile;

    @Option(name = "--clientfile", description = "The script file for the client to parse")
    public static  String clientfile;

    @Option(name = "--tests", description = "For the client only, optional argument to list out the tests you" +
            " want to run, should be comma separated names")
    public static String tests;

    public static void main(String[] args) {
        SingleCommand<TestMain> cmd = SingleCommand.singleCommand(TestMain.class);
        cmd.parse(args);
        ServerThread st = new ServerThread(port, serverfile);
        st.start();

        try {
            Thread.sleep(2000); // wait for server to start up, 2 seconds to be safe
            if (tests != null) new JavaTCPClient().run(clientfile, "localhost", port, debug, Arrays.asList(tests.split(",")));
            else new JavaTCPClient().run(clientfile, "localhost", port, debug, new ArrayList<>());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
