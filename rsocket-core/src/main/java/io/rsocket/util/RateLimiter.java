package io.rsocket.util;

public class RateLimiter {
  private boolean first = true;
  private long deferredCredits = 0;
  private long outstandingCredits = 0;
  private long rateLimit;
  private long defaultRateLimit;

  public RateLimiter(long defaultRateLimit) {
    this.defaultRateLimit = defaultRateLimit;
  }

  public synchronized boolean onRequest(long request) {
    if (request == Long.MAX_VALUE) {
      deferredCredits = Long.MAX_VALUE;
    } else {
      deferredCredits += request;
    }

    if (deferredCredits < 0) {
      throw new AssertionError("deferredCredits < 0: " + deferredCredits);
    }

    if (!first) {
      return false;
    }

    first = false;
    rateLimit = request == Long.MAX_VALUE ? defaultRateLimit : request;

    return true;
  }

  public synchronized long nextRequest() {
    long request = 0;
    if (outstandingCredits == 0) {
      request = Math.min(deferredCredits, rateLimit);
      outstandingCredits += request;
    }

    // TODO below low watermark, request more before exhaustion

    deferredCredits -= request;

    return request;
  }

  public synchronized void onNext() {
    outstandingCredits--;
  }
}
