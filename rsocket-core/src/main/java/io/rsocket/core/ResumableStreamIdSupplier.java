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

import io.netty.util.collection.IntObjectMap;

final class ResumableStreamIdSupplier implements StreamIdSupplier {
  final StreamIdSupplier origin;
  final IntObjectMap<?> streamIds;

  ResumableStreamIdSupplier(StreamIdSupplier origin, IntObjectMap<?> streamIds) {
    this.origin = origin;
    this.streamIds = streamIds;
  }

  @Override
  public int nextStreamId() {
    int streamId;
    do {
      streamId = origin.nextStreamId();
    } while (streamIds.containsKey(streamId));
    return streamId;
  }

  public boolean isBeforeOrCurrent(int streamId) {
    return origin.isBeforeOrCurrent(streamId);
  }
}
