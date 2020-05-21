package io.rsocket.addons;

import io.rsocket.Closeable;
import io.rsocket.RSocket;

public interface RSocketPool extends Closeable {

  RSocket select();
}
