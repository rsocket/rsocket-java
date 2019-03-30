/*
 * Copyright 2015-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {
  private boolean isDisposed;
  private final Map<ByteBuf, ServerRSocketSession> sessions = new ConcurrentHashMap<>();

  public ServerRSocketSession save(ServerRSocketSession session) {
    if (isDisposed) {
      session.dispose();
    } else {
      ByteBuf token = session.token().retain();
      session
          .onClose()
          .doOnSuccess(
              v -> {
                sessions.remove(token);
                token.release();
              })
          .subscribe();
      ServerRSocketSession prev = sessions.put(token, session);
      if (prev != null) {
        prev.dispose();
      }
    }
    return session;
  }

  public Optional<ServerRSocketSession> get(ByteBuf resumeToken) {
    return Optional.ofNullable(sessions.get(resumeToken));
  }

  public void dispose() {
    isDisposed = true;
    sessions.values().forEach(ServerRSocketSession::dispose);
  }
}
