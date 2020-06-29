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

package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameType;
import reactor.core.publisher.SignalType;
import reactor.util.annotation.Nullable;

/** @deprecated in favor of the improved {@link LeaseTracker} class */
@Deprecated
public interface LeaseStats extends LeaseTracker {

  @Override
  default void onAccept(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {
    onEvent(EventType.ACCEPT);
  }

  @Override
  default void onReject(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {
    onEvent(EventType.REJECT);
  }

  @Override
  default void onRelease(int stream, SignalType terminalSignal) {}

  @Override
  default void onClose() {
    onEvent(EventType.TERMINATE);
  }

  void onEvent(EventType eventType);

  enum EventType {
    ACCEPT,
    REJECT,
    TERMINATE
  }
}
