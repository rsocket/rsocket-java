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

import java.util.Arrays;
import reactor.core.Disposable;

/** Utilities for working with the {@link Disposable} type. */
public final class DisposableUtil {

  private DisposableUtil() {}

  /**
   * Calls the {@link Disposable#dispose()} method if the instance is not null. If any exceptions
   * are thrown during disposal, suppress them.
   *
   * @param disposables the {@link Disposable}s to dispose
   */
  public static void disposeQuietly(Disposable... disposables) {
    Arrays.stream(disposables)
        .forEach(
            disposable -> {
              try {
                disposable.dispose();
              } catch (RuntimeException e) {
                // Suppress any exceptions during disposal
              }
            });
  }
}
