package io.rsocket.util;

import java.time.Duration;

public class KeepAliveData {
  private final Duration tickPeriod;
  private final Duration timeout;

  public KeepAliveData(long tickPeriodMillis, long timeoutMillis) {
    this(Duration.ofMillis(tickPeriodMillis), Duration.ofMillis(timeoutMillis));
  }

  public KeepAliveData(Duration tickPeriod, Duration timeout) {
    this.tickPeriod = tickPeriod;
    this.timeout = timeout;
  }

  public Duration getTickPeriod() {
    return tickPeriod;
  }

  public Duration getTimeout() {
    return timeout;
  }
}
