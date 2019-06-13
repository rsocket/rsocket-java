package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.rsocket.Availability;
import reactor.core.Disposable;

public interface RequesterLeaseHandler extends Availability, Disposable {

  boolean useLease();

  Exception leaseError();

  void receive(ByteBuf leaseFrame);

  void dispose();

  RequesterLeaseHandler Noop =
      new RequesterLeaseHandler() {
        @Override
        public boolean useLease() {
          return true;
        }

        @Override
        public Exception leaseError() {
          throw new AssertionError("Error not possible with NOOP leaseSender handler");
        }

        @Override
        public void receive(ByteBuf leaseFrame) {}

        @Override
        public void dispose() {}

        @Override
        public double availability() {
          return 1.0;
        }
      };
}
