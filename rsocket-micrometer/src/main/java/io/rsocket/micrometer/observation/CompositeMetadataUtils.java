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

import io.micrometer.core.lang.Nullable;
import io.netty.buffer.ByteBuf;
import io.rsocket.metadata.CompositeMetadata;

final class CompositeMetadataUtils {

  private CompositeMetadataUtils() {
    throw new IllegalStateException("Can't instantiate a utility class");
  }

  @Nullable
  static ByteBuf extract(ByteBuf metadata, String key) {
    final CompositeMetadata compositeMetadata = new CompositeMetadata(metadata, false);
    for (CompositeMetadata.Entry entry : compositeMetadata) {
      final String entryKey = entry.getMimeType();
      if (key.equals(entryKey)) {
        return entry.getContent();
      }
    }
    return null;
  }
}
