package io.reactivesocket.aeron.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * No more needed to type Logger LOGGER = LoggerFactory.getLogger....
 */
public interface Loggable {
    default void info(String message, Object... args) {
        logger().debug(message, args);
    }

    default void error(String message, Throwable t) {
        logger().error(message, t);
    }

    default void debug(String message, Object... args) {
        logger().debug(message, args);
    }

    static final ConcurrentHashMap<Class, Logger> loggers = new ConcurrentHashMap<>();

    default Logger logger() {
        return loggers.computeIfAbsent(getClass(), LoggerFactory::getLogger);
    }
}
