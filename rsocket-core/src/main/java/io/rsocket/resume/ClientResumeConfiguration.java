package io.rsocket.resume;

import java.time.Duration;
import java.util.function.Supplier;

public class ClientResumeConfiguration {
  private final Duration sessionDuration;
  private final int cacheSizeFrames;
  private final Supplier<ResumeStrategy> resumeStrategy;

  public ClientResumeConfiguration(
      Duration sessionDuration, int cacheSizeFrames, Supplier<ResumeStrategy> resumeStrategy) {
    this.sessionDuration = sessionDuration;
    this.cacheSizeFrames = cacheSizeFrames;
    this.resumeStrategy = resumeStrategy;
  }

  public Duration sessionDuration() {
    return sessionDuration;
  }

  public int cacheSizeFrames() {
    return cacheSizeFrames;
  }

  public Supplier<ResumeStrategy> resumptionStrategy() {
    return resumeStrategy;
  }
}
