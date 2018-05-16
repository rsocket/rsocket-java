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

package io.rsocket.internal;

import io.netty.util.ReferenceCountUtil;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.core.publisher.UnicastProcessor;

public final class SwitchTransform<T, R> extends Flux<R> {

  final Publisher<? extends T> source;
  final BiFunction<T, Flux<T>, Publisher<? extends R>> transformer;

  public SwitchTransform(
      Publisher<? extends T> source, BiFunction<T, Flux<T>, Publisher<? extends R>> transformer) {
    this.source = Objects.requireNonNull(source, "source");
    this.transformer = Objects.requireNonNull(transformer, "transformer");
  }

  @Override
  public void subscribe(CoreSubscriber<? super R> actual) {
    source.subscribe(new SwitchTransformSubscriber<>(actual, transformer));
  }

  static final class SwitchTransformSubscriber<T, R>
      implements CoreSubscriber<T> {
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<SwitchTransformSubscriber> ONCE =
        AtomicIntegerFieldUpdater.newUpdater(SwitchTransformSubscriber.class, "once");

    final CoreSubscriber<? super R> actual;
    final BiFunction<T, Flux<T>, Publisher<? extends R>> transformer;
    final UnicastProcessor<T> processor = UnicastProcessor.create();
    Subscription s;
    volatile int once;

    SwitchTransformSubscriber(
        CoreSubscriber<? super R> actual,
        BiFunction<T, Flux<T>, Publisher<? extends R>> transformer) {
      this.actual = actual;
      this.transformer = transformer;
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (Operators.validate(this.s, s)) {
        this.s = s;
        processor.onSubscribe(s);
      }
    }

    @Override
    public void onNext(T t) {
      if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
        try {
          Publisher<? extends R> result =
              Objects.requireNonNull(
                  transformer.apply(t, processor), "The transformer returned a null value");
          result.subscribe(actual);
        } catch (Throwable e) {
          onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
          ReferenceCountUtil.safeRelease(t);
          return;
        }
      }
      processor.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
      processor.onError(t);
    }

    @Override
    public void onComplete() {
      processor.onComplete();
    }
  }
}
