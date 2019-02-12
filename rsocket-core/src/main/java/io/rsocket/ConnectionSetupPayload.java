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
import io.rsocket.frame.SetupFrameFlyweight;

/**
 * Exposed to server for determination of ResponderRSocket based on mime types and SETUP
 * metadata/data
 */
public abstract class ConnectionSetupPayload extends AbstractReferenceCounted implements Payload {

  public static ConnectionSetupPayload create(final ByteBuf setupFrame) {
    return new DefaultConnectionSetupPayload(setupFrame);
  }

  public abstract int keepAliveInterval();

  public abstract int keepAliveMaxLifetime();

  public abstract String metadataMimeType();

  public abstract String dataMimeType();

  public abstract int getFlags();

  public abstract boolean willClientHonorLease();

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
    private final ByteBuf setupFrame;

    public DefaultConnectionSetupPayload(ByteBuf setupFrame) {
      this.setupFrame = setupFrame;
    }

    @Override
    public boolean hasMetadata() {
      return FrameHeaderFlyweight.hasMetadata(setupFrame);
    }

    @Override
    public int keepAliveInterval() {
      return SetupFrameFlyweight.keepAliveInterval(setupFrame);
    }

    @Override
    public int keepAliveMaxLifetime() {
      return SetupFrameFlyweight.keepAliveMaxLifetime(setupFrame);
    }

    @Override
    public String metadataMimeType() {
      return SetupFrameFlyweight.metadataMimeType(setupFrame);
    }

    @Override
    public String dataMimeType() {
      return SetupFrameFlyweight.dataMimeType(setupFrame);
    }

    @Override
    public int getFlags() {
      return FrameHeaderFlyweight.flags(setupFrame);
    }

    @Override
    public boolean willClientHonorLease() {
      return SetupFrameFlyweight.honorLease(setupFrame);
    }

    @Override
    public ConnectionSetupPayload touch() {
      setupFrame.touch();
      return this;
    }

    @Override
    public ConnectionSetupPayload touch(Object hint) {
      setupFrame.touch(hint);
      return this;
    }

    @Override
    protected void deallocate() {
      setupFrame.release();
    }

    @Override
    public ByteBuf sliceMetadata() {
      return SetupFrameFlyweight.metadata(setupFrame);
    }

    @Override
    public ByteBuf sliceData() {
      return SetupFrameFlyweight.data(setupFrame);
    }
  }
}
