package io.rsocket.lease;

import io.rsocket.plugins.RequestInterceptor;

public interface TrackingLeaseSender extends LeaseSender, RequestInterceptor {}
