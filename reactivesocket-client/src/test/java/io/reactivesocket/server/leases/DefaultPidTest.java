package io.reactivesocket.server.leases;

import org.junit.Test;

public class DefaultPidTest {
    @Test
    public void testPid() {
        Pid pid = new DefaultPid(() -> 100, () -> 0.5, () -> 0.5, () -> 10);

        System.out.println(pid.getOutput());

        for (int i = 0; i < 200; i++) {
            pid.update(i);
            System.out.println(pid.getOutput());
        }
    }
}