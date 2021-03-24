package io.rsocket.core;

/** Handler which enables async lease permits issuing */
interface LeasePermitHandler {

  /**
   * Called by {@link RequesterLeaseTracker} when there is an available lease
   *
   * @return {@code true} to indicate that lease permit was consumed successfully
   */
  boolean handlePermit();

  /**
   * Called by {@link RequesterLeaseTracker} when there are no lease permit available at the moment
   * and the list of awaiting {@link LeasePermitHandler} reached the configured limit
   *
   * @param t associated lease permit rejection exception
   */
  void handlePermitError(Throwable t);
}
