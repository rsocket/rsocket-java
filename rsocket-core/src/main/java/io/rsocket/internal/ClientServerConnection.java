package io.rsocket.internal;

import io.rsocket.DuplexConnection;
import io.rsocket.resume.ResumeAwareConnection;
import io.rsocket.resume.ResumeStateHolder;
import io.rsocket.util.DuplexConnectionProxy;
import reactor.core.publisher.Flux;

class ClientServerConnection extends DuplexConnectionProxy implements ResumeAwareConnection {

  private final DuplexConnection resumeAware;

  public ClientServerConnection(DuplexConnection delegate, DuplexConnection resumeAware) {
    super(delegate);
    this.resumeAware = resumeAware;
  }

  @Override
  public Flux<Long> receiveResumePositions(ResumeStateHolder resumeStateHolder) {
    return resumeAware instanceof ResumeAwareConnection
        ? ((ResumeAwareConnection) resumeAware).receiveResumePositions(resumeStateHolder)
        : Flux.never();
  }
}
