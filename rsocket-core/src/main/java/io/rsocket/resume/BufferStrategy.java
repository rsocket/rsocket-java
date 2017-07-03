package io.rsocket.resume;

import io.rsocket.Frame;

public interface BufferStrategy {
  int cost(Frame f);

  static BufferStrategy size() {
    return f -> ResumeUtil.offset(f);
  }

  static BufferStrategy frames() {
    return f -> 1;
  }
}
