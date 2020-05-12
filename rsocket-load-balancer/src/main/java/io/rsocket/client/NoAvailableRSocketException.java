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

package io.rsocket.client;

@Deprecated
/** An exception that indicates that no RSocket was available. */
public final class NoAvailableRSocketException extends Exception {

  /**
   * The single instance of this type. <b>Note</b> that it is initialized without any stack trace.
   */
  public static final NoAvailableRSocketException INSTANCE;

  private static final long serialVersionUID = -2785312562743351184L;

  static {
    NoAvailableRSocketException exception = new NoAvailableRSocketException();
    exception.setStackTrace(
        new StackTraceElement[] {
          new StackTraceElement(exception.getClass().getName(), "<init>", null, -1)
        });

    INSTANCE = exception;
  }

  private NoAvailableRSocketException() {};
}
