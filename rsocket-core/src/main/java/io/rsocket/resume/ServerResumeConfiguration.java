package io.rsocket.resume;

import java.time.Duration;

public class ServerResumeConfiguration {
  private final Duration sessionDuration;
  private final int cacheSizeFrames;

  public ServerResumeConfiguration(Duration sessionDuration, int cacheSizeFrames) {
    this.sessionDuration = sessionDuration;
    this.cacheSizeFrames = cacheSizeFrames;
  }

  public Duration sessionDuration() {
    return sessionDuration;
  }

  public int cacheSizeFrames() {
    return cacheSizeFrames;
  }
}
