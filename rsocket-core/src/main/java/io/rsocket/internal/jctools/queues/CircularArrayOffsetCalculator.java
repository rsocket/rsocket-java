/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.internal.jctools.queues;

import static io.rsocket.internal.jctools.util.UnsafeRefArrayAccess.REF_ARRAY_BASE;
import static io.rsocket.internal.jctools.util.UnsafeRefArrayAccess.REF_ELEMENT_SHIFT;

import io.rsocket.internal.jctools.util.InternalAPI;

@InternalAPI
public final class CircularArrayOffsetCalculator {
  @SuppressWarnings("unchecked")
  public static <E> E[] allocate(int capacity) {
    return (E[]) new Object[capacity];
  }

  /**
   * @param index desirable element index
   * @param mask (length - 1)
   * @return the offset in bytes within the array for a given index.
   */
  public static long calcElementOffset(long index, long mask) {
    return REF_ARRAY_BASE + ((index & mask) << REF_ELEMENT_SHIFT);
  }
}
