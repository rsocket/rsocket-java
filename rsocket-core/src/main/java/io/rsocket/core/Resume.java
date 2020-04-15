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
import io.rsocket.resume.ExponentialBackoffResumeStrategy;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.resume.ResumableFramesStore;
import io.rsocket.resume.ResumeStrategy;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

public class Resume {
  Duration sessionDuration = Duration.ofMinutes(2);
  Duration streamTimeout = Duration.ofSeconds(10);
  boolean cleanupStoreOnKeepAlive;
  Function<? super ByteBuf, ? extends ResumableFramesStore> storeFactory;

  private Supplier<ByteBuf> tokenSupplier = ResumeFrameFlyweight::generateResumeToken;
  private Supplier<ResumeStrategy> resumeStrategySupplier =
      () -> new ExponentialBackoffResumeStrategy(Duration.ofSeconds(1), Duration.ofSeconds(16), 2);

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

  public Resume resumeStrategy(Supplier<ResumeStrategy> supplier) {
    this.resumeStrategySupplier = supplier;
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

  Supplier<ResumeStrategy> getResumeStrategySupplier() {
    return resumeStrategySupplier;
  }
}
