package io.rsocket.core;

import io.rsocket.Payload;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Queue;
import java.util.function.Supplier;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

/**
 * This class is a temporary solution to workaround memory leaks related to bug in reactor. <br>
 * For more information, pleas see https://github.com/reactor/reactor-core/pull/2114
 */
@SuppressWarnings("unchecked")
final class LimitRateOperator {

  static final Constructor<Flux<Payload>> fluxPublishOnCtr;

  static {
    try {
      Class<?> fluxPublishOnClass = Class.forName("reactor.core.publisher.FluxPublishOn");
      fluxPublishOnCtr =
          (Constructor<Flux<Payload>>) fluxPublishOnClass.getDeclaredConstructors()[0];
      fluxPublishOnCtr.setAccessible(true);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  static Flux<Payload> applyLimitRateOperator(Flux<Payload> source, int limit) {
    Supplier<Queue<Payload>> queueSupplier =
        () -> {
          Queue<Payload> queue = Queues.<Payload>get(limit).get();
          return new CleanOnClearQueueDecorator(queue);
        };

    try {
      return fluxPublishOnCtr.newInstance(
          source, Schedulers.immediate(), true, limit, limit, queueSupplier);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
