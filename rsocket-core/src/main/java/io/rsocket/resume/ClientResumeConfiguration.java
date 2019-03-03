package io.rsocket.resume;

import java.time.Duration;
import java.util.function.Supplier;

public class ClientResumeConfiguration {
  private final Duration sessionDuration;
  private final Supplier<ResumeStrategy> resumeStrategy;
  private final ResumeStore resumeStore;
  private final Duration resumeStreamTimeout;

  public ClientResumeConfiguration(
      Duration sessionDuration,
      Supplier<ResumeStrategy> resumeStrategy,
      ResumeStore resumeStore, Duration resumeStreamTimeout) {
    this.sessionDuration = sessionDuration;
    this.resumeStrategy = resumeStrategy;
    this.resumeStore = resumeStore;
    this.resumeStreamTimeout = resumeStreamTimeout;
  }

  public Duration sessionDuration() {
    return sessionDuration;
  }

  public Supplier<ResumeStrategy> resumptionStrategy() {
    return resumeStrategy;
  }

  public ResumeStore resumeStore() {
    return resumeStore;
  }

  public Duration resumeStreamTimeout() {
    return resumeStreamTimeout;
  }
}
