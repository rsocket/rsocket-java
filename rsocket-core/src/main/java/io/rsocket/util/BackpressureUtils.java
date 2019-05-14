package io.rsocket.util;

import io.rsocket.internal.LimitableRequestPublisher;
import io.rsocket.internal.SynchronizedIntObjectHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class BackpressureUtils {

  public static void shareRequest(
      long requested,
      SynchronizedIntObjectHashMap<LimitableRequestPublisher> limitableSubscriptions) {
    try {

      if (limitableSubscriptions.isEmpty()) {
        return;
      }
      ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
      Object[] activeSubscriptions;
      int size;
      synchronized (limitableSubscriptions) {
        activeSubscriptions = limitableSubscriptions.getValuesCopy();
        size = limitableSubscriptions.size();
      }
      int length = activeSubscriptions.length;
      int randomStartIndex = threadLocalRandom.nextInt(0, size);
      long requestPerItem = requested / size;

      requestPerItem = requestPerItem == 0 ? 1L : requestPerItem;

      for (int i = 0; i < length && requested >= 0; i++) {
        LimitableRequestPublisher lrp =
            (LimitableRequestPublisher) activeSubscriptions[randomStartIndex];
        if (lrp != null) {
          lrp.increaseInternalLimit(requestPerItem);
          requested -= requestPerItem;
        }
        randomStartIndex++;
        if (randomStartIndex == length) {
          randomStartIndex = 0;
        }
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
}
