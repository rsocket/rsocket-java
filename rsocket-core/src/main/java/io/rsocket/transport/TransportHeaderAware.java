package io.rsocket.transport;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Extension interface to support Transports with headers at the transport layer, e.g. Websockets,
 * Http2.
 */
public interface TransportHeaderAware {
  void setTransportHeaders(Supplier<Map<String, String>> transportHeaders);
}
