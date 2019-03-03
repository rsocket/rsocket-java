package io.rsocket.resume;

import java.time.Duration;
import java.util.function.Function;

public class ServerResumeConfiguration {
  private final Duration sessionDuration;
  private final Duration resumeStreamTimeout;
  private final Function<? super ResumeToken, ? extends ResumeStore> resumeStoreFactory;

  public ServerResumeConfiguration(Duration sessionDuration,
                                   Duration resumeStreamTimeout,
                                   Function<? super ResumeToken, ? extends ResumeStore> resumeStoreFactory) {
    this.sessionDuration = sessionDuration;
    this.resumeStreamTimeout = resumeStreamTimeout;
    this.resumeStoreFactory = resumeStoreFactory;
  }

  public Duration sessionDuration() {
    return sessionDuration;
  }

  public Duration resumeStreamTimeout() {
    return resumeStreamTimeout;
  }

  public Function<? super ResumeToken, ? extends ResumeStore> resumeStoreFactory() {
    return resumeStoreFactory;
  }
}
