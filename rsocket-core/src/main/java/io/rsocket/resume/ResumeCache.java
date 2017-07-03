package io.rsocket.resume;

import io.rsocket.Frame;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Flux;

public class ResumeCache {
  private final BufferStrategy strategy;
  private final int maxBufferSize;

  private LinkedHashMap<Integer, Frame> frames = new LinkedHashMap<>();
  private int lastRemotePosition = 0;
  private int currentPosition = 0;
  private int bufferSize;

  public ResumeCache(BufferStrategy strategy, int maxBufferSize) {
    this.strategy = strategy;
    this.maxBufferSize = maxBufferSize;
  }

  public void updateRemotePosition(int remotePosition) {
    if (remotePosition > currentPosition) {
      throw new IllegalStateException(
          "Remote ahead of " + lastRemotePosition + " , expected " + remotePosition);
    }

    if (remotePosition == lastRemotePosition) {
      return;
    }

    if (remotePosition < lastRemotePosition) {
      throw new IllegalStateException(
          "Remote position moved back from " + lastRemotePosition + " to " + remotePosition);
    }

    lastRemotePosition = remotePosition;

    Iterator<Map.Entry<Integer, Frame>> positions = frames.entrySet().iterator();

    while (positions.hasNext()) {
      Map.Entry<Integer, Frame> cachePosition = positions.next();

      if (cachePosition.getKey() <= remotePosition) {
        positions.remove();
        bufferSize -= strategy.cost(cachePosition.getValue());
        cachePosition.getValue().release();
      }

      // TODO check for a bad position
    }
  }

  public void sent(Frame frame) {
    if (ResumeUtil.isTracked(frame)) {
      frames.put(currentPosition, frame.retain());
      bufferSize += strategy.cost(frame);

      currentPosition += ResumeUtil.offset(frame);

      if (frames.size() > maxBufferSize) {
        Frame f = frames.remove(first(frames));
        bufferSize -= strategy.cost(f);
        f.release();
      }
    }
  }

  private int first(LinkedHashMap<Integer, Frame> frames) {
    return frames.keySet().iterator().next();
  }

  public Flux<Frame> resend(int remotePosition) {
    updateRemotePosition(remotePosition);

    if (remotePosition == currentPosition) {
      return Flux.empty();
    }

    List<Frame> resend = new ArrayList<>();

    Iterator<Map.Entry<Integer, Frame>> positions = frames.entrySet().iterator();

    while (positions.hasNext()) {
      Map.Entry<Integer, Frame> cachePosition = positions.next();

      if (remotePosition < cachePosition.getKey()) {
        resend.add(cachePosition.getValue().retain());
      }

      // TODO error handling
    }

    return Flux.fromIterable(resend);
  }

  public int getCurrentPosition() {
    return currentPosition;
  }

  public int getRemotePosition() {
    return lastRemotePosition;
  }

  public int getEarliestResendPosition() {
    if (frames.isEmpty()) {
      return currentPosition;
    } else {
      return first(frames);
    }
  }

  public int size() {
    return bufferSize;
  }
}
