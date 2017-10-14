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

package io.rsocket.transport.local;

import io.rsocket.Closeable;
import io.rsocket.test.extension.AbstractExtension;
import io.rsocket.test.extension.SetupResource;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalSetupResource extends SetupResource<String, Closeable> {
  private static final AtomicInteger uniqueNameGenerator = new AtomicInteger();

  private LocalSetupResource() {
    super(
        () -> "test" + uniqueNameGenerator.incrementAndGet(),
        (address, server) -> LocalClientTransport.create(address),
        LocalServerTransport::create);
  }

  public static class Extension extends AbstractExtension {
    @Override
    protected Class<?> type() {
      return LocalSetupResource.class;
    }
  }
}
