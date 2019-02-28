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

package io.rsocket.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;

public class EmptyPayload implements Payload {
  public static final EmptyPayload INSTANCE = new EmptyPayload();

  private EmptyPayload() {}

  @Override
  public boolean hasMetadata() {
    return false;
  }

  @Override
  public ByteBuf sliceMetadata() {
    return Unpooled.EMPTY_BUFFER;
  }

  @Override
  public ByteBuf sliceData() {
    return Unpooled.EMPTY_BUFFER;
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
  public int refCnt() {
    return 1;
  }

  @Override
  public EmptyPayload retain() {
    return this;
  }

  @Override
  public EmptyPayload retain(int increment) {
    return this;
  }

  @Override
  public EmptyPayload touch() {
    return this;
  }

  @Override
  public EmptyPayload touch(Object hint) {
    return this;
  }

  @Override
  public boolean release() {
    return false;
  }

  @Override
  public boolean release(int decrement) {
    return false;
  }
}
