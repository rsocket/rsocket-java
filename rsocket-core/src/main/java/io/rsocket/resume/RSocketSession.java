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
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import reactor.core.publisher.Mono;

public interface RSocketSession<T> extends Closeable {

  ByteBuf token();

  //  ResumableDuplexConnection resumableConnection();

  RSocketSession continueWith(T ConnectionFactory);

  RSocketSession resumeWith(ByteBuf resumeFrame);

  void reconnect(DuplexConnection connection);

  @Override
  default Mono<Void> onClose() {
    //    return resumableConnection().onClose();
    return null;
  }

  @Override
  default void dispose() {
    //    resumableConnection().dispose();
  }

  @Override
  default boolean isDisposed() {
    //    return resumableConnection().isDisposed();
    return true;
  }
}
