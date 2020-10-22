package io.rsocket.loadbalance;

import io.rsocket.core.RSocketConnector;

/**
 * Extension for {@link LoadbalanceStrategy} which allows pre-setup {@link RSocketConnector} for
 * {@link LoadbalanceStrategy} needs
 *
 * @since 1.1
 */
public interface ClientLoadbalanceStrategy extends LoadbalanceStrategy {

  void initialize(RSocketConnector connector);
}
