package io.rsocket.tckdrivers.test;

import static org.junit.Assert.assertEquals;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.tckdrivers.client.JavaClientDriver;
import io.rsocket.tckdrivers.common.ServerThread;
import io.rsocket.transport.netty.client.TcpClientTransport;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TckTest {


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

  private String name; // Test name
  private JavaClientDriver jd; // javaclientdriver object for running the given test
  private List<String> test; // test instructions/commands

  public TckTest(String name, JavaClientDriver jd, List<String> test) {
    this.name = name;
    this.jd = jd;
    this.test = test;
  }

  /**
   * Runs the test.
   */
  @Test
  public void TckTestRunner() {
    JavaClientDriver.TestResult result = JavaClientDriver.TestResult.FAIL;
    try {
      result = this.jd.parse(this.test.subList(1, this.test.size()), this.name);
    } catch (Exception e) {
      e.printStackTrace();
    }

    assertEquals(result, JavaClientDriver.TestResult.PASS);
  }

  /**
   * A function that reads all the server/client test files from "path". For
   * each server file, it starts a server. It parses each client file and
   * create a parameterized test.
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
        String serverFileName = serverPrefix + file.getName().replaceFirst(clientPrefix, "");

        File f = new File(path + serverFileName);
        if (f.exists() && !f.isDirectory()) {

          //starting a server
          ServerThread st = new ServerThread(currentPort, path + serverFileName);
          st.start();
          st.awaitStart();

          try {
            RSocket client =
                createClient(new URI("tcp://" + hostname + ":" + currentPort + "/rs"));
            JavaClientDriver jd = new JavaClientDriver(() -> client);

            BufferedReader reader = new BufferedReader(new FileReader(file));
            List<List<String>> tests = new ArrayList<>();
            List<String> test = new ArrayList<>();
            String line = reader.readLine();

            //Parsing the input client file to read all the tests
            while (line != null) {
              switch (line) {
                case "!":
                  tests.add(test);
                  test = new ArrayList<>();
                  break;
                default:
                  test.add(line);
                  break;
              }
              line = reader.readLine();
            }
            tests.add(test);
            tests = tests.subList(1, tests.size()); // remove the first list, which is empty

            for (List<String> t : tests) {

              String name = "";
              name = t.get(0).split("%%")[1];
              System.out.println(name);

              Object[] testObject = new Object[3];
              testObject[0] = name;
              testObject[1] = jd;
              testObject[2] = t;
              testData.add(testObject);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }

          currentPort++;

        } else {
          System.out.println("SERVER file does not exists");
        }
      }
    }
    return testData;
  }

  /**
   * A function that creates a RSocket on a new TCP connection.
   *
   * @return a RSocket
   */
  public static RSocket createClient(URI uri) {
    if ("tcp".equals(uri.getScheme())) {
      return RSocketFactory.connect()
          .transport(TcpClientTransport.create(uri.getHost(), uri.getPort()))
          .start()
          .block();
    } else {
      throw new UnsupportedOperationException("uri unsupported: " + uri);
    }
  }
}
