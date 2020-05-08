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

package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.rsocket.core.DefaultConnectionSetupPayload;
import reactor.util.annotation.Nullable;

/**
 * Exposes information from the {@code SETUP} frame to a server, as well as to client responders.
 */
public abstract class ConnectionSetupPayload extends AbstractReferenceCounted implements Payload {

  public abstract String metadataMimeType();

  public abstract String dataMimeType();

  public abstract int keepAliveInterval();

  public abstract int keepAliveMaxLifetime();

  public abstract int getFlags();

  public abstract boolean willClientHonorLease();

  public abstract boolean isResumeEnabled();

  @Nullable
  public abstract ByteBuf resumeToken();

  @Override
  public ConnectionSetupPayload retain() {
    super.retain();
    return this;
  }

  @Override
  public ConnectionSetupPayload retain(int increment) {
    super.retain(increment);
    return this;
  }

  @Override
  public abstract ConnectionSetupPayload touch();

  /**
   * Create a {@code ConnectionSetupPayload}.
   *
   * @deprecated as of 1.0 RC7. Please, use {@link
   *     DefaultConnectionSetupPayload#DefaultConnectionSetupPayload(ByteBuf)
   *     DefaultConnectionSetupPayload} constructor.
   */
  @Deprecated
  public static ConnectionSetupPayload create(final ByteBuf setupFrame) {
    return new DefaultConnectionSetupPayload(setupFrame);
  }
}
