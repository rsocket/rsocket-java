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

package io.rsocket;

final class StreamIdSupplier {

  private int streamId;

  private StreamIdSupplier(int streamId) {
    this.streamId = streamId;
  }

  synchronized int nextStreamId() {
    streamId += 2;
    return streamId;
  }

  synchronized boolean isBeforeOrCurrent(int streamId) {
    return this.streamId >= streamId && streamId > 0;
  }

  static StreamIdSupplier clientSupplier() {
    return new StreamIdSupplier(-1);
  }

  static StreamIdSupplier serverSupplier() {
    return new StreamIdSupplier(0);
  }
}
