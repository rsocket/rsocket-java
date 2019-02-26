package io.rsocket.util;

import io.rsocket.DuplexConnection;
import io.rsocket.keepalive.KeepAliveConnection;
import io.rsocket.resume.ResumeToken;

public interface ClientSetup {
  /*Provide different connections for SETUP / RESUME cases*/
  DuplexConnection wrappedConnection(KeepAliveConnection duplexConnection);

  /*Provide different resume tokens for SETUP / RESUME cases*/
  ResumeToken resumeToken();
}
