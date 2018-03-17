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

package io.rsocket.aeron.internal;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import java.util.function.Function;

/** */
public interface AeronWrapper {
  Aeron getAeron();

  void availableImageHandler(Function<Image, Boolean> handler);

  void unavailableImageHandlers(Function<Image, Boolean> handler);

  default Subscription addSubscription(String channel, int streamId) {
    return getAeron().addSubscription(channel, streamId);
  }

  default Publication addPublication(String channel, int streamId) {
    return getAeron().addPublication(channel, streamId);
  }

  default void close() {
    getAeron().close();
  }
}
