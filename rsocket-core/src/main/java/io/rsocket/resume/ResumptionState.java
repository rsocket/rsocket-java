package io.rsocket.resume;

import java.util.Objects;

class ResumptionState {
  private final long pos;
  private final long impliedPos;

  ResumptionState(long pos, long impliedPos) {
    this.pos = pos;
    this.impliedPos = impliedPos;
  }

  public static ResumptionState fromServer(long impliedPos) {
    return new ResumptionState(-1, impliedPos);
  }

  public static ResumptionState fromClient(long pos, long impliedPos) {
    return new ResumptionState(pos, impliedPos);
  }

  public boolean isServer() {
    return pos < 0;
  }

  public long position() {
    return pos;
  }

  public long impliedPosition() {
    return impliedPos;
  }

  @Override
  public String toString() {
    return "ResumptionState{" + "pos=" + pos + ", impliedPos=" + impliedPos + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ResumptionState that = (ResumptionState) o;
    return pos == that.pos && impliedPos == that.impliedPos;
  }

  @Override
  public int hashCode() {
    return Objects.hash(pos, impliedPos);
  }
}
