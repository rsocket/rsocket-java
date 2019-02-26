package io.rsocket.resume;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {
  private volatile boolean isDisposed;
  private final Map<ResumeToken, ServerRSocketSession> sessions =
      new ConcurrentHashMap<>();

  public ServerRSocketSession save(ServerRSocketSession session) {
    if (isDisposed) {
      session.dispose();
    } else {
      ResumeToken token = session.token();
      session.onClose().doOnSuccess(v -> sessions.remove(token)).subscribe();
      ServerRSocketSession prev = sessions.put(token, session);
      if (prev != null) {
        prev.dispose();
      }
    }
    return session;
  }

  public Optional<ServerRSocketSession> get(ResumeToken resumeToken) {
    return Optional.ofNullable(sessions.get(resumeToken));
  }

  public void dispose() {
    isDisposed = true;
    sessions.values().forEach(ServerRSocketSession::dispose);
    sessions.clear();
  }
}
