package io.rsocket.util;

import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.internal.SynchronizedIntObjectHashMap;

public class BackpressureUtils {

  public static void shareRequest(
      long requested,
      SynchronizedIntObjectHashMap<LimitableRequestPublisher> limitableSubscriptions) {
    try {
      if (limitableSubscriptions.isEmpty()) {
        return;
      }
      Object[] activeSubscriptions;
      activeSubscriptions = limitableSubscriptions.getValuesCopy();
      int length = activeSubscriptions.length;

      for (int i = 0; i < length; i++) {
        LimitableRequestPublisher lrp = (LimitableRequestPublisher) activeSubscriptions[i];
        if (lrp != null) {
          lrp.increaseInternalLimit(requested);
        }
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
}
