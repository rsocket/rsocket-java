/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.aeron.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    default void trace(String message, Object... args) {
        logger().trace(message, args);
    }

    default boolean isTraceEnabled() {
        if (Constants.TRACING_ENABLED) {
            return logger().isTraceEnabled();
        } else {
            return false;
        }
    }

    default Logger logger() {
        return LoggerFactory.getLogger(getClass());
    }
}
