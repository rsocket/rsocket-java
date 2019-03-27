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

import static io.rsocket.frame.FrameHeaderFlyweight.FLAGS_M;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.rsocket.Frame.Setup;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.framing.FrameType;

/**
 * Exposed to server for determination of ResponderRSocket based on mime types and SETUP
 * metadata/data
 */
public abstract class ConnectionSetupPayload extends AbstractReferenceCounted implements Payload {

  public static ConnectionSetupPayload create(final Frame setupFrame) {
    Frame.ensureFrameType(FrameType.SETUP, setupFrame);
    return new DefaultConnectionSetupPayload(setupFrame);
  }

  public abstract int keepAliveInterval();

  public abstract int keepAliveMaxLifetime();

  public abstract String metadataMimeType();

  public abstract String dataMimeType();

  public abstract int getFlags();

  public boolean willClientHonorLease() {
    return Frame.isFlagSet(getFlags(), SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE);
  }

  @Override
  public boolean hasMetadata() {
    return Frame.isFlagSet(getFlags(), FLAGS_M);
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

    private final Frame setupFrame;

    public DefaultConnectionSetupPayload(final Frame setupFrame) {
      this.setupFrame = setupFrame;
    }

    @Override
    public int keepAliveInterval() {
      return SetupFrameFlyweight.keepaliveInterval(setupFrame.content());
    }

    @Override
    public int keepAliveMaxLifetime() {
      return SetupFrameFlyweight.maxLifetime(setupFrame.content());
    }

    @Override
    public String metadataMimeType() {
      return Setup.metadataMimeType(setupFrame);
    }

    @Override
    public String dataMimeType() {
      return Setup.dataMimeType(setupFrame);
    }

    @Override
    public ByteBuf sliceData() {
      return setupFrame.sliceData();
    }

    @Override
    public ByteBuf sliceMetadata() {
      return setupFrame.sliceMetadata();
    }

    @Override
    public int getFlags() {
      return Setup.getFlags(setupFrame);
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
  }
}
