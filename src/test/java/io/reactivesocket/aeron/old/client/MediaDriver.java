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
package io.reactivesocket.aeron.old.client;

import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;

/**
 * Created by rroeser on 8/16/15.
 */
public class MediaDriver {
    public static void main(String... args) {
        ThreadingMode threadingMode = ThreadingMode.SHARED;

        boolean dedicated = Boolean.getBoolean("dedicated");

        if (true) {
            threadingMode = ThreadingMode.DEDICATED;
        }

        System.out.println("ThreadingMode => " + threadingMode);

        final uk.co.real_logic.aeron.driver.MediaDriver.Context ctx = new uk.co.real_logic.aeron.driver.MediaDriver.Context()
                .threadingMode(threadingMode)
                .dirsDeleteOnStart(true)
                .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
                .receiverIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
                .senderIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1));

        final uk.co.real_logic.aeron.driver.MediaDriver ignored = uk.co.real_logic.aeron.driver.MediaDriver.launch(ctx);

    }
}
