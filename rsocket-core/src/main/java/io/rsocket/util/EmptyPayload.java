package io.rsocket.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;

public class EmptyPayload implements Payload {
  public static final EmptyPayload INSTANCE = new EmptyPayload();

  private EmptyPayload() {
  }

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
