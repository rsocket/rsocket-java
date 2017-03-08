/*
 * Copyright 2016 Netflix, Inc.
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
package io.reactivesocket.internal;

public final class FlowControlHelper {

    private FlowControlHelper() {
    }

    /**
     * Increment {@code existing} value with {@code toAdd}.
     *
     * @param existing Existing value of {@code requestN}
     * @param toAdd Value to increment by.
     *
     * @return New {@code requestN} value capped at {@link Integer#MAX_VALUE}.
     */
    public static int incrementRequestN(int existing, int toAdd) {
        if (existing == Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        final int u = existing + toAdd;
        return u < 0 ? Integer.MAX_VALUE : u;
    }

    /**
     * Increment {@code existing} value with {@code toAdd}.
     *
     * @param existing Existing value of {@code requestN}
     * @param toAdd Value to increment by.
     *
     * @return New {@code requestN} value capped at {@link Integer#MAX_VALUE}.
     */
    public static int incrementRequestN(int existing, long toAdd) {
        if (existing == Integer.MAX_VALUE || toAdd >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        final int u = existing + (int)toAdd; // Safe downcast: Since toAdd can not be > Integer.MAX_VALUE here.
        return u < 0 ? Integer.MAX_VALUE : u;
    }

    /**
     * Increment existing by add and if there is overflow return Long.MAX_VALUE
     */
    public static long incrementRequestN(long existing, long toAdd) {
        long l = existing + toAdd;
        return l < 0 ? Long.MAX_VALUE : l;
    }
}
