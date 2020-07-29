/*
 * Copyright 2015-2020 the original author or authors.
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
package io.rsocket.core;

import io.netty.buffer.CompositeByteBuf;
import io.rsocket.Payload;
import java.util.concurrent.CancellationException;
import reactor.util.annotation.Nullable;

interface RequesterFrameHandler extends FrameHandler {

  void handlePayload(Payload payload);

  @Override
  default void handleCancel() {
    handleError(
        new CancellationException(
            "Cancellation was received but should not be possible for current request type"));
  }

  @Override
  default void handleRequestN(long n) {
    // no ops
  }

  @Nullable
  CompositeByteBuf getFrames();

  void setFrames(@Nullable CompositeByteBuf reassembledFrames);
}
