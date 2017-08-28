package io.rsocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import reactor.util.context.Context;

public class JsonContextEncoder extends BaseContextEncoder {
  private ObjectMapper om;

  public JsonContextEncoder() {
    this.om = new ObjectMapper();
  }

  @Override protected ByteBuffer encode(Context context) throws IOException {
    Map<Object, Object> m = new LinkedHashMap<>();

    context.stream().forEach(e -> m.put(e.getKey(), e.getValue()));

    return ByteBuffer.wrap(om.writeValueAsBytes(m));
  }
}
