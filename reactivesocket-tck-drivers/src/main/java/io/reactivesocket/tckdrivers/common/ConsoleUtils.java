package io.reactivesocket.tckdrivers.common;

/**
 * This class handles everything that gets printed to the console
 */
public class ConsoleUtils {

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String ANSI_BLUE = "\u001B[34m";

    /**
     * Logs something at the info level
     */
    public static void info(String s) {
        System.out.println("INFO: " + s);
    }

    /**
     * Logs a successful event
     */
    public static void success(String s) {
        System.out.println(ANSI_GREEN + "SUCCESS: " + s + ANSI_RESET);
    }

    /**
     * Logs a failure event
     */
    public static void failure(String s) {
        System.out.println(ANSI_RED + "FAILURE: " + s + ANSI_RESET);
    }

    /**
     * Logs an error
     */
    public static void error(String s) {
        System.out.println("ERROR: " + s);
    }

    /**
     * Logs a time
     */
    public static void time(String s) {
        System.out.println(ANSI_CYAN + "TIME: " + s + ANSI_RESET);
    }

    /**
     * Logs the initial payload the server has received
     */
    public static void initialPayload(String s) {

    }

    /**
     * Logs the start of a test
     */
    public static void teststart(String s) {
        System.out.println(ANSI_BLUE + "TEST STARTING: " + s + ANSI_RESET);
    }

}
