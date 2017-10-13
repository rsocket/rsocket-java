package io.rsocket;

import io.rsocket.lease.LeaseRSocketRef;
import io.rsocket.util.RSocketProxy;
import java.util.Optional;
import reactor.util.annotation.NonNull;

class LeaseRSocketImpl extends RSocketProxy implements LeaseRSocket {

  private final Optional<LeaseRSocketRef> leaseControl;

  public LeaseRSocketImpl(@NonNull RSocket source, @NonNull Optional<LeaseRSocketRef> leaseControl) {
    super(source);
    this.leaseControl = leaseControl;
  }

  @Override
  public Optional<LeaseRSocketRef> leaseControl() {
    return leaseControl;
  }
}
