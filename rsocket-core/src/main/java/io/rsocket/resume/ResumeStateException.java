package io.rsocket.resume;

class ResumeStateException extends RuntimeException {
  private static final long serialVersionUID = -5393753463377588732L;
  private final ResumptionState local;
  private final ResumptionState remote;

  public ResumeStateException(ResumptionState local, ResumptionState remote) {
    this.local = local;
    this.remote = remote;
  }

  public ResumptionState localState() {
    return local;
  }

  public ResumptionState remoteState() {
    return remote;
  }
}
