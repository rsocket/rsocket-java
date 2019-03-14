/*
 * Copyright 2015-2019 the original author or authors.
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
