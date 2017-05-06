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

package io.rsocket.lease;

import io.rsocket.AbstractRSocket;
import io.rsocket.RSocket;
import io.rsocket.test.util.MockRSocket;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public abstract class AbstractSocketRule<T extends RSocket> extends ExternalResource {

    private T socket;
    private MockRSocket mockSocket;
    private RSocket requestHandler;

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                init();
                base.evaluate();
            }
        };
    }

    protected void init() {
        if (null == requestHandler) {
            requestHandler = new AbstractRSocket() { };
        }
        mockSocket = new MockRSocket(requestHandler);
        socket = newSocket(mockSocket);
    }

    public MockRSocket getMockSocket() {
        return mockSocket;
    }

    public T getRSocket() {
        return socket;
    }

    protected abstract T newSocket(RSocket delegate);

    protected void setRequestHandler(RSocket socket) {
        requestHandler = socket;
    }
}
