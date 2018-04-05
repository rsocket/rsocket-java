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

package io.rsocket.util;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import java.util.function.Function;

/** A factory for creating {@link Recycler}s. */
public final class RecyclerFactory {

  /**
   * Creates a new {@link Recycler}.
   *
   * @param newObjectCreator the {@link Function} to create a new object
   * @param <T> the type being recycled.
   * @return the {@link Recycler}
   * @throws NullPointerException if {@code newObjectCreator} is {@code null}
   */
  public static <T> Recycler<T> createRecycler(Function<Handle<T>, T> newObjectCreator) {
    Objects.requireNonNull(newObjectCreator, "newObjectCreator must not be null");

    return new Recycler<T>() {

      @Override
      protected T newObject(Handle<T> handle) {
        return newObjectCreator.apply(handle);
      }
    };
  }
}
