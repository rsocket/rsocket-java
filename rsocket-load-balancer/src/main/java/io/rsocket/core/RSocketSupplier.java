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

package io.rsocket.core;

import io.rsocket.RSocket;
import reactor.core.publisher.Mono;

public class RSocketSupplier {

  final Mono<RSocket> source;
  final Object hashObject;

  public RSocketSupplier(Mono<RSocket> source) {
    this(source, source);
  }

  public RSocketSupplier(Mono<RSocket> source, Object hashObject) {
    this.source = source;
    this.hashObject = hashObject;
  }

  public Mono<RSocket> source() {
    return source;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RSocketSupplier that = (RSocketSupplier) o;

    return hashObject.equals(that.hashObject);
  }

  @Override
  public int hashCode() {
    return hashObject.hashCode();
  }
}
