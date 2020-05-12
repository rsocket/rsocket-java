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

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * An abstract implementation of {@link RSocket}. All request handling methods emit {@link
 * UnsupportedOperationException} and hence must be overridden to provide a valid implementation.
 *
 * @deprecated as of 1.0 in favor of implementing {@link RSocket} directly which has default
 *     methods.
 */
@Deprecated
public abstract class AbstractRSocket implements RSocket {

  private final MonoProcessor<Void> onClose = MonoProcessor.create();

  @Override
  public void dispose() {
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }
}
