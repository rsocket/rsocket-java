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

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.RSocket;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestSubscriber;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reactivestreams.Subscriber;

public abstract class AbstractSocketRule<T extends RSocket> extends ExternalResource {

  protected TestDuplexConnection connection;
  protected Subscriber<Void> connectSub;
  protected T socket;
  protected LeaksTrackingByteBufAllocator allocator;

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        allocator = LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
        connection = new TestDuplexConnection(allocator);
        connectSub = TestSubscriber.create();
        init();
        base.evaluate();
      }
    };
  }

  protected void init() {
    socket = newRSocket();
  }

  protected abstract T newRSocket();

  public ByteBufAllocator alloc() {
    return allocator;
  }

  public void assertHasNoLeaks() {
    allocator.assertHasNoLeaks();
  }
}
