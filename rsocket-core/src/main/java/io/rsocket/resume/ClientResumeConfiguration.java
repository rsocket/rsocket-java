/*
 * Copyright 2015-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
