package io.reactivesocket.tckdrivers.main;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import io.reactivesocket.tckdrivers.client.JavaTCPClient;
import io.reactivesocket.tckdrivers.server.JavaTCPServer;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class is used to run both the server and the client, depending on the options given
 */
@Command(name = "reactivesocket-driver", description = "This runs the client and servers that use the driver")
public class Main {

    @Option(name = "--debug", description = "set if you want frame level output")
    public static boolean debug;

    @Option(name = "--server", description = "set if you want to run the server")
    public static boolean server;

    @Option(name = "--client", description = "set if you want to run the client")
    public static boolean client;

    @Option(name = "--host", description = "The host to connect to for the client")
    public static String host;

    @Option(name = "--port", description = "The port")
    public static int port;

    @Option(name = "--file", description = "The script file to parse, make sure to give the client and server the " +
            "correct files")
    public static String file;

    @Option(name = "--tests", description = "For the client only, optional argument to list out the tests you" +
            " want to run, should be comma separated names")
    public static String tests;

    public static void main(String[] args) {
        SingleCommand<Main> cmd = SingleCommand.singleCommand(Main.class);
        cmd.parse(args);
        if (server) {
            JavaTCPServer.run(file, port);
        } else if (client) {
            try {
                if (tests != null) JavaTCPClient.run(file, host, port, debug, Arrays.asList(tests.split(",")));
                else JavaTCPClient.run(file, host, port, debug, new ArrayList<>());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
