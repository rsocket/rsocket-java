/*
 * Copyright 2015-2020 the original author or authors.
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

package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.SetupFrameFlyweight;

/**
 * Default implementation of {@link ConnectionSetupPayload}. Primarily for internal use within
 * RSocket Java but may be created in an application, e.g. for testing purposes.
 */
public class DefaultConnectionSetupPayload extends ConnectionSetupPayload {

  private final ByteBuf setupFrame;

  public DefaultConnectionSetupPayload(ByteBuf setupFrame) {
    this.setupFrame = setupFrame;
  }

  @Override
  public boolean hasMetadata() {
    return FrameHeaderFlyweight.hasMetadata(setupFrame);
  }

  @Override
  public ByteBuf sliceMetadata() {
    final ByteBuf metadata = SetupFrameFlyweight.metadata(setupFrame);
    return metadata == null ? Unpooled.EMPTY_BUFFER : metadata;
  }

  @Override
  public ByteBuf sliceData() {
    return SetupFrameFlyweight.data(setupFrame);
  }

  @Override
  public ByteBuf data() {
    return sliceData();
  }

  @Override
  public ByteBuf metadata() {
    return sliceMetadata();
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
  public int keepAliveInterval() {
    return SetupFrameFlyweight.keepAliveInterval(setupFrame);
  }

  @Override
  public int keepAliveMaxLifetime() {
    return SetupFrameFlyweight.keepAliveMaxLifetime(setupFrame);
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
  public boolean isResumeEnabled() {
    return SetupFrameFlyweight.resumeEnabled(setupFrame);
  }

  @Override
  public ByteBuf resumeToken() {
    return SetupFrameFlyweight.resumeToken(setupFrame);
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
