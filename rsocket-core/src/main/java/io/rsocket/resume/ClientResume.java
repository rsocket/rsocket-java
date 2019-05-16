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

import io.netty.buffer.ByteBuf;
import java.time.Duration;

public class ClientResume {
  private final Duration sessionDuration;
  private final ByteBuf resumeToken;

  public ClientResume(Duration sessionDuration, ByteBuf resumeToken) {
    this.sessionDuration = sessionDuration;
    this.resumeToken = resumeToken;
  }

  public Duration sessionDuration() {
    return sessionDuration;
  }

  public ByteBuf resumeToken() {
    return resumeToken;
  }
}
