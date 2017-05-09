package io.rsocket;

import reactor.core.publisher.Mono;

/**
 *
 */
public interface Startable {
    Mono<Void> start();
}
