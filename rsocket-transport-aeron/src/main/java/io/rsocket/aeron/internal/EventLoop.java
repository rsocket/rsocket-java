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

package io.rsocket.aeron.internal;

import java.util.function.IntSupplier;

/** Interface for an EventLoop used by Aeron */
public interface EventLoop {
  /**
   * Executes an IntSupplier that returns a number greater than 0 if it wants the the event loop to
   * keep processing items, and zero its okay for the eventloop to execute an idle strategy
   *
   * @param r signal for roughly how many items could be processed.
   * @return whether items could be processed
   */
  boolean execute(IntSupplier r);
}
