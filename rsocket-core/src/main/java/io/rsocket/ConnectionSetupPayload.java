/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.rsocket.frame.FrameHeaderFlyweight;

/**
 * Exposed to server for determination of ResponderRSocket based on mime types and SETUP metadata/data
 */
public abstract class ConnectionSetupPayload extends AbstractReferenceCounted implements Payload {

  public static ConnectionSetupPayload create(final ByteBuf setupFrame) {
    return null;
  }

  public abstract int keepAliveInterval();

  public abstract int keepAliveMaxLifetime();

  public abstract String metadataMimeType();

  public abstract String dataMimeType();

  public abstract int getFlags();

  public boolean willClientHonorLease() {
    return false;
  }

  @Override
  public boolean hasMetadata() {
    return FrameHeaderFlyweight.hasMetadata(null);
  }

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

  public abstract ConnectionSetupPayload touch();

  public abstract ConnectionSetupPayload touch(Object hint);

  private static final class DefaultConnectionSetupPayload extends ConnectionSetupPayload {

    @Override
    public int keepAliveInterval() {
      return 0;
    }

    @Override
    public int keepAliveMaxLifetime() {
      return 0;
    }

    @Override
    public String metadataMimeType() {
      return null;
    }

    @Override
    public String dataMimeType() {
      return null;
    }

    @Override
    public int getFlags() {
      return 0;
    }

    @Override
    public ConnectionSetupPayload touch() {
      return null;
    }

    @Override
    public ConnectionSetupPayload touch(Object hint) {
      return null;
    }

    @Override
    protected void deallocate() {}

    @Override
    public ByteBuf sliceMetadata() {
      return null;
    }

    @Override
    public ByteBuf sliceData() {
      return null;
    }
  }
}
