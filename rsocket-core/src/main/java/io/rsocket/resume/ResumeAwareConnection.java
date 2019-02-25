package io.rsocket.resume;

import io.rsocket.DuplexConnection;
import reactor.core.publisher.Flux;

public interface ResumeAwareConnection extends DuplexConnection {

  Flux<Long> receiveResumePositions(ResumeStateHolder resumeStateHolder);
}
