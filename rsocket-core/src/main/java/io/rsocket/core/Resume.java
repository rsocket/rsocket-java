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
import io.rsocket.frame.ResumeFrameCodec;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.resume.ResumableFramesStore;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.retry.Retry;

/**
 * Simple holder of configuration settings for the RSocket Resume capability. This can be used to
 * configure an {@link RSocketConnector} or an {@link RSocketServer} except for {@link
 * #retry(Retry)} and {@link #token(Supplier)} which apply only to the client side.
 */
public class Resume {
  private static final Logger logger = LoggerFactory.getLogger(Resume.class);

  private Duration sessionDuration = Duration.ofMinutes(2);

  /* Storage */
  private boolean cleanupStoreOnKeepAlive;
  private Function<? super ByteBuf, ? extends ResumableFramesStore> storeFactory;
  private Duration streamTimeout = Duration.ofSeconds(10);

  /* Client only */
  private Supplier<ByteBuf> tokenSupplier = ResumeFrameCodec::generateResumeToken;
  private Retry retry =
      Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
          .maxBackoff(Duration.ofSeconds(16))
          .jitter(1.0)
          .doBeforeRetry(signal -> logger.debug("Connection error", signal.failure()));

  public Resume() {}

  /**
   * The maximum time for a client to keep trying to reconnect. During this time client and server
   * continue to store unsent frames to keep the session warm and ready to resume.
   *
   * <p>By default this is set to 2 minutes.
   *
   * @param sessionDuration the max duration for a session
   * @return the same instance for method chaining
   */
  public Resume sessionDuration(Duration sessionDuration) {
    this.sessionDuration = Objects.requireNonNull(sessionDuration);
    return this;
  }

  /**
   * When this property is enabled, hints from {@code KEEPALIVE} frames about how much data has been
   * received by the other side, is used to proactively clean frames from the {@link
   * #storeFactory(Function) store}.
   *
   * <p>By default this is set to {@code false} in which case information from {@code KEEPALIVE} is
   * ignored and old frames from the store are removed only when the store runs out of space.
   *
   * @return the same instance for method chaining
   */
  public Resume cleanupStoreOnKeepAlive() {
    this.cleanupStoreOnKeepAlive = true;
    return this;
  }

  /**
   * Configure a factory to create the storage for buffering (or persisting) a window of frames that
   * may need to be sent again to resume after a dropped connection.
   *
   * <p>By default {@link InMemoryResumableFramesStore} is used with its cache size set to 100,000
   * bytes. When the cache fills up, the oldest frames are gradually removed to create space for new
   * ones.
   *
   * @param storeFactory the factory to use to create the store
   * @return the same instance for method chaining
   */
  public Resume storeFactory(
      Function<? super ByteBuf, ? extends ResumableFramesStore> storeFactory) {
    this.storeFactory = storeFactory;
    return this;
  }

  /**
   * A {@link reactor.core.publisher.Flux#timeout(Duration) timeout} value to apply to the resumed
   * session stream obtained from the {@link #storeFactory(Function) store} after a reconnect. The
   * resume stream must not take longer than the specified time to emit each frame.
   *
   * <p>By default this is set to 10 seconds.
   *
   * @param streamTimeout the timeout value for resuming a session stream
   * @return the same instance for method chaining
   */
  public Resume streamTimeout(Duration streamTimeout) {
    this.streamTimeout = Objects.requireNonNull(streamTimeout);
    return this;
  }

  /**
   * Configure the logic for reconnecting. This setting is for use with {@link
   * RSocketConnector#resume(Resume)} on the client side only.
   *
   * <p>By default this is set to:
   *
   * <pre>{@code
   * Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
   *     .maxBackoff(Duration.ofSeconds(16))
   *     .jitter(1.0)
   * }</pre>
   *
   * @param retry the {@code Retry} spec to use when attempting to reconnect
   * @return the same instance for method chaining
   */
  public Resume retry(Retry retry) {
    this.retry = retry;
    return this;
  }

  /**
   * Customize the generation of the resume identification token used to resume. This setting is for
   * use with {@link RSocketConnector#resume(Resume)} on the client side only.
   *
   * <p>By default this is {@code ResumeFrameFlyweight::generateResumeToken}.
   *
   * @param supplier a custom generator for a resume identification token
   * @return the same instance for method chaining
   */
  public Resume token(Supplier<ByteBuf> supplier) {
    this.tokenSupplier = supplier;
    return this;
  }

  // Package private accessors

  Duration getSessionDuration() {
    return sessionDuration;
  }

  boolean isCleanupStoreOnKeepAlive() {
    return cleanupStoreOnKeepAlive;
  }

  Function<? super ByteBuf, ? extends ResumableFramesStore> getStoreFactory(String tag) {
    return storeFactory != null
        ? storeFactory
        : token -> new InMemoryResumableFramesStore(tag, 100_000);
  }

  Duration getStreamTimeout() {
    return streamTimeout;
  }

  Retry getRetry() {
    return retry;
  }

  Supplier<ByteBuf> getTokenSupplier() {
    return tokenSupplier;
  }
}
