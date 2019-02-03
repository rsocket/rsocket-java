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

package io.rsocket.resume;

import io.netty.buffer.ByteBuf;

/**
 * Calculates the cost of a Frame when stored in the ResumeCache. Two obvious and provided
 * strategies are simple frame counts and size in bytes.
 */
public interface ResumePositionCounter {
  int cost(ByteBuf f);

  static ResumePositionCounter size() {
    return ResumeUtil::offset;
  }

  static ResumePositionCounter frames() {
    return f -> 1;
  }
}
