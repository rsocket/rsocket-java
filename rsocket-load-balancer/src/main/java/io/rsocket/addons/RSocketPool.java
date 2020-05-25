package io.rsocket.addons;

import io.rsocket.RSocket;
import reactor.core.Disposable;

public interface RSocketPool extends Disposable {

  RSocket select();
}
