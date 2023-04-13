/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.micrometer.observation;

import io.rsocket.frame.FrameType;

/**
 * Default {@link RSocketRequesterObservationConvention} implementation.
 *
 * @author Marcin Grzejszczak
 * @since 1.1.4
 */
class DefaultRSocketObservationConvention {

  private final RSocketContext rSocketContext;

  public DefaultRSocketObservationConvention(RSocketContext rSocketContext) {
    this.rSocketContext = rSocketContext;
  }

  String getName() {
    if (this.rSocketContext.frameType == FrameType.REQUEST_FNF) {
      return "rsocket.fnf";
    } else if (this.rSocketContext.frameType == FrameType.REQUEST_STREAM) {
      return "rsocket.stream";
    } else if (this.rSocketContext.frameType == FrameType.REQUEST_CHANNEL) {
      return "rsocket.channel";
    }
    return "%s";
  }

  protected RSocketContext getRSocketContext() {
    return this.rSocketContext;
  }
}
