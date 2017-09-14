package io.rsocket.resume;

import io.rsocket.Frame;

/**
 * Calculates the cost of a Frame when stored in the ResumeCache. Two obvious and provided
 * strategies are simple frame counts and size in bytes.
 */
public interface ResumePositionCounter {
  int cost(Frame f);

  static ResumePositionCounter size() {
    return ResumeUtil::offset;
  }

  static ResumePositionCounter frames() {
    return f -> 1;
  }
}
