/*
 * Copyright 2015-2020 the original author or authors.
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
package io.rsocket.test.util;

import java.net.SocketAddress;
import java.util.Objects;

public final class TestLocalSocketAddress extends SocketAddress {

  private static final long serialVersionUID = 2608695156052100164L;

  private final String name;

  /**
   * Creates a new instance.
   *
   * @param name the name representing the address
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public TestLocalSocketAddress(String name) {
    this.name = Objects.requireNonNull(name, "name must not be null");
  }

  /** Return the name for this connection. */
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "[local address] " + name;
  }
}
