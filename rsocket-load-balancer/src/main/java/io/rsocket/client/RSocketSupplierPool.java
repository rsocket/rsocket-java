package io.rsocket.client;

import io.rsocket.Closeable;
import io.rsocket.client.filter.RSocketSupplier;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class RSocketSupplierPool
    implements Supplier<Optional<RSocketSupplier>>, Consumer<RSocketSupplier>, Closeable {
  private static final Logger logger = LoggerFactory.getLogger(RSocketSupplierPool.class);
  private static final int EFFORT = 5;

  private final ArrayList<RSocketSupplier> factoryPool;
  private final ArrayList<RSocketSupplier> leasedSuppliers;

  private final MonoProcessor<Void> onClose;

  public RSocketSupplierPool(Publisher<? extends Collection<RSocketSupplier>> publisher) {
    this.onClose = MonoProcessor.create();
    this.factoryPool = new ArrayList<>();
    this.leasedSuppliers = new ArrayList<>();

    Disposable disposable =
        Flux.from(publisher)
            .doOnNext(this::handleNewFactories)
            .onErrorResume(
                t -> {
                  logger.error("error streaming RSocketSuppliers", t);
                  return Mono.delay(Duration.ofSeconds(10)).then(Mono.error(t));
                })
            .subscribe();

    onClose.doFinally(s -> disposable.dispose()).subscribe();
  }

  private synchronized void handleNewFactories(Collection<RSocketSupplier> newFactories) {
    Set<RSocketSupplier> current = new HashSet<>(factoryPool.size() + leasedSuppliers.size());
    current.addAll(factoryPool);
    current.addAll(leasedSuppliers);

    Set<RSocketSupplier> removed = new HashSet<>(current);
    removed.removeAll(newFactories);

    Set<RSocketSupplier> added = new HashSet<>(newFactories);
    added.removeAll(current);

    boolean changed = false;
    Iterator<RSocketSupplier> it0 = leasedSuppliers.iterator();
    while (it0.hasNext()) {
      RSocketSupplier supplier = it0.next();
      if (removed.contains(supplier)) {
        it0.remove();
        try {
          changed = true;
          supplier.dispose();
        } catch (Exception e) {
          logger.warn("Exception while closing a RSocket", e);
        }
      }
    }

    Iterator<RSocketSupplier> it1 = factoryPool.iterator();
    while (it1.hasNext()) {
      RSocketSupplier supplier = it1.next();
      if (removed.contains(supplier)) {
        it1.remove();
        try {
          changed = true;
          supplier.dispose();
        } catch (Exception e) {
          logger.warn("Exception while closing a RSocket", e);
        }
      }
    }

    factoryPool.addAll(added);
    if (!added.isEmpty()) {
      changed = true;
    }

    if (changed && logger.isDebugEnabled()) {
      StringBuilder msgBuilder = new StringBuilder();
      msgBuilder
          .append("\nUpdated active factories (size: ")
          .append(factoryPool.size())
          .append(")\n");
      for (RSocketSupplier f : factoryPool) {
        msgBuilder.append(" + ").append(f).append('\n');
      }
      msgBuilder.append("Active sockets:\n");
      for (RSocketSupplier socket : leasedSuppliers) {
        msgBuilder.append(" + ").append(socket).append('\n');
      }
      logger.debug(msgBuilder.toString());
    }
  }

  @Override
  public synchronized void accept(RSocketSupplier rSocketSupplier) {
    boolean contained = leasedSuppliers.remove(rSocketSupplier);
    if (contained
        && !rSocketSupplier
            .isDisposed()) { // only added leasedSupplier back to factoryPool if it's still there
      factoryPool.add(rSocketSupplier);
    }
  }

  @Override
  public synchronized Optional<RSocketSupplier> get() {
    Optional<RSocketSupplier> optional = Optional.empty();
    int poolSize = factoryPool.size();
    if (poolSize == 1) {
      RSocketSupplier rSocketSupplier = factoryPool.get(0);
      if (rSocketSupplier.availability() > 0.0) {
        factoryPool.remove(0);
        leasedSuppliers.add(rSocketSupplier);
        logger.debug("Added {} to leasedSuppliers", rSocketSupplier);
        optional = Optional.of(rSocketSupplier);
      }
    } else if (poolSize > 1) {
      Random rng = ThreadLocalRandom.current();
      int size = factoryPool.size();
      RSocketSupplier factory0 = null;
      RSocketSupplier factory1 = null;
      int i0 = 0;
      int i1 = 0;
      for (int i = 0; i < EFFORT; i++) {
        i0 = rng.nextInt(size);
        i1 = rng.nextInt(size - 1);
        if (i1 >= i0) {
          i1++;
        }
        factory0 = factoryPool.get(i0);
        factory1 = factoryPool.get(i1);
        if (factory0.availability() > 0.0 && factory1.availability() > 0.0) {
          break;
        }
      }
      if (factory0.availability() > factory1.availability()) {
        factoryPool.remove(i0);
        leasedSuppliers.add(factory0);
        logger.debug("Added {} to leasedSuppliers", factory0);
        optional = Optional.of(factory0);
      } else {
        factoryPool.remove(i1);
        leasedSuppliers.add(factory1);
        logger.debug("Added {} to leasedSuppliers", factory1);
        optional = Optional.of(factory1);
      }
    }

    return optional;
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public void dispose() {
    if (!onClose.isDisposed()) {
      onClose.onComplete();

      close(factoryPool);
      close(leasedSuppliers);
    }
  }

  private void close(Collection<RSocketSupplier> suppliers) {
    for (RSocketSupplier supplier : suppliers) {
      try {
        supplier.dispose();
      } catch (Throwable t) {
      }
    }
  }

  public synchronized int poolSize() {
    return factoryPool.size();
  }

  public synchronized boolean isPoolEmpty() {
    return factoryPool.isEmpty();
  }
}
