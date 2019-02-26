package io.rsocket.resume;

import java.time.Duration;

public class ServerResumeConfiguration {
  private final Duration sessionDuration;
  private final long cacheSizeFrames;

  public ServerResumeConfiguration(Duration sessionDuration, long cacheSizeFrames) {
    this.sessionDuration = sessionDuration;
    this.cacheSizeFrames = cacheSizeFrames;
  }

  public Duration sessionDuration() {
    return sessionDuration;
  }

  public long cacheSizeFrames() {
    return cacheSizeFrames;
  }
}
