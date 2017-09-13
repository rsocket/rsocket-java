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

package io.rsocket.aeron.internal;

import io.aeron.Aeron;
import io.aeron.Image;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;

/** */
public class DefaultAeronWrapper implements AeronWrapper {
  private Set<Function<Image, Boolean>> availableImageHandlers;
  private Set<Function<Image, Boolean>> unavailableImageHandlers;

  private Aeron aeron;

  public DefaultAeronWrapper() {
    this.availableImageHandlers = new CopyOnWriteArraySet<>();
    this.unavailableImageHandlers = new CopyOnWriteArraySet<>();

    Aeron.Context ctx = new Aeron.Context();

    ctx.availableImageHandler(this::availableImageHandler);
    ctx.unavailableImageHandler(this::unavailableImageHandler);

    this.aeron = Aeron.connect(ctx);
  }

  public Aeron getAeron() {
    return aeron;
  }

  public void availableImageHandler(Function<Image, Boolean> handler) {
    availableImageHandlers.add(handler);
  }

  public void unavailableImageHandlers(Function<Image, Boolean> handler) {
    unavailableImageHandlers.add(handler);
  }

  private void availableImageHandler(Image image) {
    Iterator<Function<Image, Boolean>> iterator = availableImageHandlers.iterator();

    Set<Function<Image, Boolean>> itemsToRemove = new HashSet<>();
    while (iterator.hasNext()) {
      Function<Image, Boolean> handler = iterator.next();
      if (handler.apply(image)) {
        itemsToRemove.add(handler);
      }
    }

    availableImageHandlers.removeAll(itemsToRemove);
  }

  private void unavailableImageHandler(Image image) {
    unavailableImageHandlers.removeIf(handler -> handler.apply(image));
  }
}
