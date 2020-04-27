/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.ResumeFrameFlyweight;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.resume.ResumableFramesStore;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.retry.Retry;

public class Resume {
  private static final Logger logger = LoggerFactory.getLogger(Resume.class);

  private Duration sessionDuration = Duration.ofMinutes(2);
  private Duration streamTimeout = Duration.ofSeconds(10);
  private boolean cleanupStoreOnKeepAlive;
  private Function<? super ByteBuf, ? extends ResumableFramesStore> storeFactory;

  private Supplier<ByteBuf> tokenSupplier = ResumeFrameFlyweight::generateResumeToken;
  private Retry retry =
      Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
          .maxBackoff(Duration.ofSeconds(16))
          .jitter(1.0)
          .doBeforeRetry(signal -> logger.debug("Connection error", signal.failure()));

  public Resume() {}

  public Resume sessionDuration(Duration sessionDuration) {
    this.sessionDuration = sessionDuration;
    return this;
  }

  public Resume streamTimeout(Duration streamTimeout) {
    this.streamTimeout = streamTimeout;
    return this;
  }

  public Resume cleanupStoreOnKeepAlive() {
    this.cleanupStoreOnKeepAlive = true;
    return this;
  }

  public Resume storeFactory(
      Function<? super ByteBuf, ? extends ResumableFramesStore> storeFactory) {
    this.storeFactory = storeFactory;
    return this;
  }

  public Resume token(Supplier<ByteBuf> supplier) {
    this.tokenSupplier = supplier;
    return this;
  }

  public Resume retry(Retry retry) {
    this.retry = retry;
    return this;
  }

  Duration getSessionDuration() {
    return sessionDuration;
  }

  Duration getStreamTimeout() {
    return streamTimeout;
  }

  boolean isCleanupStoreOnKeepAlive() {
    return cleanupStoreOnKeepAlive;
  }

  Function<? super ByteBuf, ? extends ResumableFramesStore> getStoreFactory(String tag) {
    return storeFactory != null
        ? storeFactory
        : token -> new InMemoryResumableFramesStore(tag, 100_000);
  }

  Supplier<ByteBuf> getTokenSupplier() {
    return tokenSupplier;
  }

  Retry getRetry() {
    return retry;
  }
}
