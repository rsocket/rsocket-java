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

import static io.rsocket.internal.jctools.queues.UnsafeRefArrayAccess.REF_ARRAY_BASE;
import static io.rsocket.internal.jctools.queues.UnsafeRefArrayAccess.REF_ELEMENT_SHIFT;

/** This is used for method substitution in the LinkedArray classes code generation. */
final class LinkedArrayQueueUtil {
  static int length(Object[] buf) {
    return buf.length;
  }

  /**
   * This method assumes index is actually (index << 1) because lower bit is used for resize. This
   * is compensated for by reducing the element shift. The computation is constant folded, so
   * there's no cost.
   */
  static long modifiedCalcCircularRefElementOffset(long index, long mask) {
    return REF_ARRAY_BASE + ((index & mask) << (REF_ELEMENT_SHIFT - 1));
  }

  static long nextArrayOffset(Object[] curr) {
    return REF_ARRAY_BASE + ((long) (length(curr) - 1) << REF_ELEMENT_SHIFT);
  }
}
