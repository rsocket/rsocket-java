package io.rsocket.frame;

import static org.junit.Assert.*;

import org.junit.Test;

public class VersionFlyweightTest {
  @Test
  public void simple() {
    int version = VersionFlyweight.encode(1, 0);
    assertEquals(1, VersionFlyweight.major(version));
    assertEquals(0, VersionFlyweight.minor(version));
    assertEquals(0x00010000, version);
    assertEquals("1.0", VersionFlyweight.toString(version));
  }

  @Test
  public void complex() {
    int version = VersionFlyweight.encode(0x1234, 0x5678);
    assertEquals(0x1234, VersionFlyweight.major(version));
    assertEquals(0x5678, VersionFlyweight.minor(version));
    assertEquals(0x12345678, version);
    assertEquals("4660.22136", VersionFlyweight.toString(version));
  }
}
