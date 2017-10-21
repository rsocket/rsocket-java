/*
 * Copyright 2016 Netflix, Inc.
 *
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

package io.rsocket;

import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestSubscriber;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reactivestreams.Subscriber;

public abstract class AbstractSocketRule<T extends RSocket> extends ExternalResource {

  protected TestDuplexConnection connection;
  protected Subscriber<Void> connectSub;
  protected T socket;
  protected ConcurrentLinkedQueue<Throwable> errors;

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        connection = new TestDuplexConnection();
        connectSub = TestSubscriber.create();
        errors = new ConcurrentLinkedQueue<>();
        init();
        base.evaluate();
      }
    };
  }

  protected void init() {
    socket = newRSocket();
  }

  protected abstract T newRSocket();

  public void assertNoConnectionErrors() {
    if (errors.size() > 1) {
      Assert.fail("No connection errors expected: " + errors.peek().toString());
    }
  }
}
