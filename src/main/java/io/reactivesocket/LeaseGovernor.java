package io.reactivesocket;

import io.reactivesocket.internal.Responder;

public interface LeaseGovernor {
    public static final LeaseGovernor NULL_LEASE_GOVERNOR = new NullLeaseGovernor();
    public static final LeaseGovernor UNLIMITED_LEASE_GOVERNOR = new UnlimitedLeaseGovernor();

    /**
     * Register a responder into the LeaseGovernor.
     * This give the responsibility to the leaseGovernor to send lease to the responder.
     *
     * @param responder the responder that will receive lease
     */
    public void register(Responder responder);

    /**
     * Unregister a responder from the LeaseGovernor.
     * Depending on the implementation, this action may trigger a rebalancing of
     * the tickets/window to the remaining responders.
     * @param responder the responder to be removed
     */
    public void unregister(Responder responder);

    /**
     * Check if the message received by the responder is valid (i.e. received during a
     * valid lease window)
     * This action my have side effect in the LeaseGovernor.
     *
     * @param responder receiving the message
     * @param frame the received frame
     * @return
     */
    public boolean accept(Responder responder, Frame frame);
}
