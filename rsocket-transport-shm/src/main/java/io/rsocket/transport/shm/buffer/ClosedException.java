package io.rsocket.transport.shm.buffer;

@SuppressWarnings("serial")
public class ClosedException extends RuntimeException {

  public ClosedException(String message) {
    super(message);
  }
}
