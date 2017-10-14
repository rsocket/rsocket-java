package io.rsocket.resume;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.junit.jupiter.api.Test;

public class ResumeTokenTest {
  @Test
  public void testFromUuid() {
    UUID x = UUID.fromString("3bac9870-3873-403a-99f4-9728aa8c7860");

    ResumeToken t = ResumeToken.bytes(ResumeToken.getBytesFromUUID(x));
    ResumeToken t2 = ResumeToken.bytes(ResumeToken.getBytesFromUUID(x));

    assertEquals("3bac98703873403a99f49728aa8c7860", t.toString());

    assertEquals(t.hashCode(), t2.hashCode());
    assertEquals(t, t2);
  }
}
