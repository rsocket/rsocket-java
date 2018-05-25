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

package io.rsocket.transport.netty;

import java.net.URI;
import java.util.Objects;

/** Utilities for dealing with with {@link URI}s */
public final class UriUtils {

  private UriUtils() {}

  /**
   * Returns the port of a URI. If the port is unset (i.e. {@code -1}) then returns the {@code
   * defaultPort}.
   *
   * @param uri the URI to extract the port from
   * @param defaultPort the default to use if the port is unset
   * @return the port of a URI or {@code defaultPort} if unset
   * @throws NullPointerException if {@code uri} is {@code null}
   */
  public static int getPort(URI uri, int defaultPort) {
    Objects.requireNonNull(uri, "uri must not be null");
    return uri.getPort() == -1 ? defaultPort : uri.getPort();
  }

  /**
   * Returns whether the URI has a secure schema. Secure is defined as being either {@code wss} or
   * {@code https}.
   *
   * @param uri the URI to examine
   * @return whether the URI has a secure schema
   * @throws NullPointerException if {@code uri} is {@code null}
   */
  public static boolean isSecure(URI uri) {
    Objects.requireNonNull(uri, "uri must not be null");
    return uri.getScheme().equals("wss") || uri.getScheme().equals("https");
  }
}
