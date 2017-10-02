package io.rsocket;

import io.rsocket.lease.LeaseControl;
import io.rsocket.util.RSocketProxy;
import java.util.Optional;
import reactor.util.annotation.NonNull;

class LeaseRSocketImpl extends RSocketProxy implements LeaseRSocket {

  private final Optional<LeaseControl> leaseControl;

  public LeaseRSocketImpl(@NonNull RSocket source, @NonNull Optional<LeaseControl> leaseControl) {
    super(source);
    this.leaseControl = leaseControl;
  }

  @Override
  public Optional<LeaseControl> leaseControl() {
    return leaseControl;
  }
}
