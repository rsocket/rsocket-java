package io.reactivesocket.aeron.client;

import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;

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
                .receiverIdleStrategy(new NoOpIdleStrategy())
                .senderIdleStrategy(new NoOpIdleStrategy());

        final uk.co.real_logic.aeron.driver.MediaDriver ignored = uk.co.real_logic.aeron.driver.MediaDriver.launch(ctx);

    }
}
