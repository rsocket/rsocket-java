package io.rsocket.tckdrivers;

import static org.junit.Assert.assertNotNull;

import io.rsocket.tckdrivers.client.JavaClientDriver;
import io.rsocket.tckdrivers.common.ServerThread;
import java.io.File;
import java.net.URI;
import java.util.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TckTest {

  private static class TckIndividualTest {
    String name; // Test name
    List<String> test; // test instructions/commands
    String testFile; // Test belong to this file. File name is without client/server prefix

    public TckIndividualTest(String name, List<String> test, String testFile) {
      this.name = name;
      this.test = test;
      this.testFile = testFile;
    }
  }

  /*
   * Start port. For every input test file a server instance will be launched.
   * For every server instance currentPort is incremented by 1
   */
  private static int currentPort = 4567;

  private static final String hostname = "localhost";

  /* Run all the test from "path". Tests files are expected to have same names
   * with a prefix "server" or "client" to indicate whether they type.
   */
  private static final String path = "src/test/resources/";

  private static final String serverPrefix = "server";
  private static final String clientPrefix = "client";
  private static HashMap<String, Integer> clientPortMap = new HashMap<String, Integer>();

  private TckIndividualTest tckTest;

  public TckTest(String testname, TckIndividualTest tckTest) {
    this.tckTest = tckTest;
  }

  /** Runs the test. */
  @Test(timeout = 10000)
  public void TckTestRunner() {

    Integer port =
        this.clientPortMap.get(
            this.tckTest.testFile); // javaclientdriver object for running the given test

    if (null == port) {

      // starting a server
      String serverFileName = serverPrefix + this.tckTest.testFile;
      ServerThread st = new ServerThread(currentPort, path + serverFileName);
      st.start();
      st.awaitStart();

      port = currentPort;
      this.clientPortMap.put(this.tckTest.testFile, port);
      currentPort++;
    }

    assertNotNull("port is not defined", port);

    try {

      JavaClientDriver jd = new JavaClientDriver(new URI("tcp://" + hostname + ":" + port + "/rs"));
      jd.runTest(this.tckTest.test.subList(1, this.tckTest.test.size()), this.tckTest.name);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * A function that reads all the server/client test files from "path". For each server file, it
   * starts a server. It parses each client file and create a parameterized test.
   *
   * @return interatable tests
   */
  @Parameters(name = "{index}: {0}")
  public static Iterable<Object[]> data() {

    File folder = new File(path);
    File[] listOfFiles = folder.listFiles();
    List<Object[]> testData = new ArrayList<Object[]>();

    for (int i = 0; i < listOfFiles.length; i++) {
      File file = listOfFiles[i];
      if (file.isFile() && file.getName().startsWith(clientPrefix)) {
        String testFile = file.getName().replaceFirst(clientPrefix, "");
        String serverFileName = serverPrefix + testFile;

        File f = new File(path + serverFileName);
        if (f.exists() && !f.isDirectory()) {

          try {

            for (List<String> t : JavaClientDriver.extractTests(file)) {

              String name = "";
              name = t.get(0).split("%%")[1];

              Object testObject[] = new Object[2];
              testObject[0] = name + " (" + testFile + ")";
              testObject[1] = new TckIndividualTest(name, t, testFile);
              testData.add(testObject);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        } else {
          System.out.println("SERVER file does not exist");
        }
      }
    }
    return testData;
  }
}
