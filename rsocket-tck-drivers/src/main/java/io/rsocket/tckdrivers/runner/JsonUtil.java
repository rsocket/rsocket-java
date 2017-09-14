package io.rsocket.tckdrivers.runner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.Payload;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class JsonUtil {
  public static Map<String, Object> parseTCKMessage(Payload content, String messageType) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      Map<String, Map<String, Object>> message =
          mapper.readValue(
              content.getDataUtf8(), new TypeReference<Map<String, Map<String, Object>>>() {});
      return message.get(messageType);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    Flux<String> server = Flux.just("1", "2", "3");

    Flux<String> client =
        server.transform(
            s -> {
              AtomicReference<Disposable> closeable = new AtomicReference<>();
              Flux<String> hotServer = s.publish().autoConnect(1, closeable::set);

              return hotServer
                  .take(1)
                  .single()
                  .flatMap(x -> Mono.just(x + "a"))
                  .doFinally(sig -> closeable.get().dispose());
            });

    System.out.println(client.blockFirst());

    Thread.sleep(1000);
  }
}
