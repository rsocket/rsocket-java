package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.rsocket.Availability;
import java.util.function.Consumer;
import reactor.core.Disposable;
import reactor.core.Disposables;

public interface ResponderLeaseHandler extends Availability {

  boolean useLease();

  Exception leaseError();

  Disposable send(Consumer<ByteBuf> leaseFrameSender);

  ResponderLeaseHandler Noop =
      new ResponderLeaseHandler() {
        @Override
        public boolean useLease() {
          return true;
        }

        @Override
        public Exception leaseError() {
          throw new AssertionError("Error not possible with NOOP leaseSender handler");
        }

        @Override
        public Disposable send(Consumer<ByteBuf> leaseFrameSender) {
          return Disposables.disposed();
        }

        @Override
        public double availability() {
          return 1.0;
        }
      };
}
