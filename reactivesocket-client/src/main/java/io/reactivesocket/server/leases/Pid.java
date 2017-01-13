package io.reactivesocket.server.leases;

public interface Pid {
    /**
     * Updates the PID with the new process value
     * @param processValue the process value
     * @return output calculated after update
     */
    double update(double processValue);

    /**
     * The current output
     * @return PID controller output
     */
    double getOutput();
}