package io.rsocket.resume;

import java.time.Duration;

public class ClientResume {
  private final Duration sessionDuration;
  private final ResumeToken resumeToken;

  public ClientResume(Duration sessionDuration, ResumeToken resumeToken) {
    this.sessionDuration = sessionDuration;
    this.resumeToken = resumeToken;
  }

  public Duration sessionDuration() {
    return sessionDuration;
  }

  public ResumeToken resumeToken() {
    return resumeToken;
  }
}
