/*
 * Copyright 2017 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.spectator.internal;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;

public final class SpectatorUtil {

    private SpectatorUtil() {
        // No instances
    }

    public static String[] mergeTags(String[] tags1, String... tags2) {
        if (tags1.length == 0) {
            return tags2;
        }
        if (tags2.length == 0) {
            return tags1;
        }

        String[] toReturn = new String[tags1.length + tags2.length];
        System.arraycopy(tags1, 0, toReturn, 0, tags1.length);
        System.arraycopy(tags2, 0, toReturn, tags1.length, tags2.length);
        return toReturn;
    }

    public static Id createId(Registry registry, String name, String monitorId, String... tags) {
        return registry.createId(name, mergeTags(tags, "id", monitorId));
    }
}
