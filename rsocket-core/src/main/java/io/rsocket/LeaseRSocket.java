package io.rsocket;

import io.rsocket.lease.LeaseRSocketRef;
import java.util.Optional;

public interface LeaseRSocket extends RSocket {

  Optional<LeaseRSocketRef> leaseControl();
}
