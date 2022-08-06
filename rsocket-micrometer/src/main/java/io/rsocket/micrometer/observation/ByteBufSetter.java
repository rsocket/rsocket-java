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

import io.micrometer.tracing.propagation.Propagator;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.rsocket.metadata.CompositeMetadataCodec;

public class ByteBufSetter implements Propagator.Setter<CompositeByteBuf> {

  @Override
  public void set(CompositeByteBuf carrier, String key, String value) {
    final ByteBufAllocator alloc = carrier.alloc();
    CompositeMetadataCodec.encodeAndAddMetadataWithCompression(
        carrier, alloc, key, ByteBufUtil.writeUtf8(alloc, value));
  }
}
