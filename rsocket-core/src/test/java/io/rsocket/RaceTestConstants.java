package io.rsocket;

public class RaceTestConstants {
  public static final int REPEATS =
      Integer.parseInt(System.getProperty("rsocket.test.race.repeats", "1000"));
}
