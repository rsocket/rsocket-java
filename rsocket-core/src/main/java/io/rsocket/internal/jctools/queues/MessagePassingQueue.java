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

public interface MessagePassingQueue<T> {
  int UNBOUNDED_CAPACITY = -1;

  interface Supplier<T> {
    T get();
  }

  interface Consumer<T> {
    void accept(T e);
  }

  interface WaitStrategy {
    int idle(int idleCounter);
  }

  interface ExitCondition {

    boolean keepRunning();
  }

  boolean offer(T e);

  T poll();

  T peek();

  int size();

  void clear();

  boolean isEmpty();

  int capacity();

  boolean relaxedOffer(T e);

  T relaxedPoll();

  T relaxedPeek();

  int drain(Consumer<T> c);

  int fill(Supplier<T> s);

  int drain(Consumer<T> c, int limit);

  int fill(Supplier<T> s, int limit);

  void drain(Consumer<T> c, WaitStrategy wait, ExitCondition exit);

  void fill(Supplier<T> s, WaitStrategy wait, ExitCondition exit);
}
