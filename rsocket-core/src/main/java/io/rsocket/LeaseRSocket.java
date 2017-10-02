package io.rsocket;

import io.rsocket.lease.LeaseControl;
import java.util.Optional;

public interface LeaseRSocket extends RSocket {

  Optional<LeaseControl> leaseControl();
}
