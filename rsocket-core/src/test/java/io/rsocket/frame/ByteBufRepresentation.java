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
package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.IllegalReferenceCountException;
import org.assertj.core.presentation.StandardRepresentation;

public final class ByteBufRepresentation extends StandardRepresentation {

  @Override
  protected String fallbackToStringOf(Object object) {
    if (object instanceof ByteBuf) {
      try {
        return ByteBufUtil.prettyHexDump((ByteBuf) object);
      } catch (IllegalReferenceCountException e) {
        //noops
      }
    }

    return super.fallbackToStringOf(object);
  }
}
