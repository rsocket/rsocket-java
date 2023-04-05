/*
 * Copyright 2015-2021 the original author or authors.
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
package io.rsocket.internal;

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public abstract class BaseDuplexConnection implements DuplexConnection {
  protected final Sinks.Empty<Void> onClose = Sinks.empty();
  protected final UnboundedProcessor sender = new UnboundedProcessor(onClose::tryEmitEmpty);

  public BaseDuplexConnection() {}

  @Override
  public void sendFrame(int streamId, ByteBuf frame) {
    if (streamId == 0) {
      sender.tryEmitPrioritized(frame);
    } else {
      sender.tryEmitNormal(frame);
    }
  }

  protected abstract void doOnClose();

  @Override
  public Mono<Void> onClose() {
    return onClose.asMono();
  }

  @Override
  public final void dispose() {
    doOnClose();
  }

  @Override
  @SuppressWarnings("ConstantConditions")
  public final boolean isDisposed() {
    return onClose.scan(Scannable.Attr.TERMINATED) || onClose.scan(Scannable.Attr.CANCELLED);
  }
}
