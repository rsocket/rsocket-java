package io.rsocket.resume;

import java.time.Duration;
import java.util.function.Supplier;

public class ClientResumeConfiguration {
  private final Duration sessionDuration;
  private final Supplier<ResumeStrategy> resumeStrategy;
  private final ResumableFramesStore resumableFramesStore;
  private final Duration resumeStreamTimeout;

  public ClientResumeConfiguration(
      Duration sessionDuration,
      Supplier<ResumeStrategy> resumeStrategy,
      ResumableFramesStore resumableFramesStore, Duration resumeStreamTimeout) {
    this.sessionDuration = sessionDuration;
    this.resumeStrategy = resumeStrategy;
    this.resumableFramesStore = resumableFramesStore;
    this.resumeStreamTimeout = resumeStreamTimeout;
  }

  public Duration sessionDuration() {
    return sessionDuration;
  }

  public Supplier<ResumeStrategy> resumptionStrategy() {
    return resumeStrategy;
  }

  public ResumableFramesStore resumeStore() {
    return resumableFramesStore;
  }

  public Duration resumeStreamTimeout() {
    return resumeStreamTimeout;
  }
}
