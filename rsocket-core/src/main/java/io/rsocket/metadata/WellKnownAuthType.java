/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.metadata;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Enumeration of Well Known Auth Types, as defined in the eponymous extension. Such auth types are
 * used in composite metadata (which can include routing and/or tracing metadata). Per
 * specification, identifiers are between 0 and 127 (inclusive).
 */
public enum WellKnownAuthType {
  UNPARSEABLE_AUTH_TYPE("UNPARSEABLE_AUTH_TYPE_DO_NOT_USE", (byte) -2),
  UNKNOWN_RESERVED_AUTH_TYPE("UNKNOWN_YET_RESERVED_DO_NOT_USE", (byte) -1),

  SIMPLE("simple", (byte) 0x00),
  BEARER("bearer", (byte) 0x01);
  // ... reserved for future use ...

  static final WellKnownAuthType[] TYPES_BY_AUTH_ID;
  static final Map<String, WellKnownAuthType> TYPES_BY_AUTH_STRING;

  static {
    // precompute an array of all valid auth ids, filling the blanks with the RESERVED enum
    TYPES_BY_AUTH_ID = new WellKnownAuthType[128]; // 0-127 inclusive
    Arrays.fill(TYPES_BY_AUTH_ID, UNKNOWN_RESERVED_AUTH_TYPE);
    // also prepare a Map of the types by auth string
    TYPES_BY_AUTH_STRING = new LinkedHashMap<>(128);

    for (WellKnownAuthType value : values()) {
      if (value.getIdentifier() >= 0) {
        TYPES_BY_AUTH_ID[value.getIdentifier()] = value;
        TYPES_BY_AUTH_STRING.put(value.getString(), value);
      }
    }
  }

  private final byte identifier;
  private final String str;

  WellKnownAuthType(String str, byte identifier) {
    this.str = str;
    this.identifier = identifier;
  }

  /**
   * Find the {@link WellKnownAuthType} for the given identifier (as an {@code int}). Valid
   * identifiers are defined to be integers between 0 and 127, inclusive. Identifiers outside of
   * this range will produce the {@link #UNPARSEABLE_AUTH_TYPE}. Additionally, some identifiers in
   * that range are still only reserved and don't have a type associated yet: this method returns
   * the {@link #UNKNOWN_RESERVED_AUTH_TYPE} when passing such an identifier, which lets call sites
   * potentially detect this and keep the original representation when transmitting the associated
   * metadata buffer.
   *
   * @param id the looked up identifier
   * @return the {@link WellKnownAuthType}, or {@link #UNKNOWN_RESERVED_AUTH_TYPE} if the id is out
   *     of the specification's range, or {@link #UNKNOWN_RESERVED_AUTH_TYPE} if the id is one that
   *     is merely reserved but unknown to this implementation.
   */
  public static WellKnownAuthType fromIdentifier(int id) {
    if (id < 0x00 || id > 0x7F) {
      return UNPARSEABLE_AUTH_TYPE;
    }
    return TYPES_BY_AUTH_ID[id];
  }

  /**
   * Find the {@link WellKnownAuthType} for the given {@link String} representation. If the
   * representation is {@code null} or doesn't match a {@link WellKnownAuthType}, the {@link
   * #UNPARSEABLE_AUTH_TYPE} is returned.
   *
   * @param authType the looked up auth type
   * @return the matching {@link WellKnownAuthType}, or {@link #UNPARSEABLE_AUTH_TYPE} if none
   *     matches
   */
  public static WellKnownAuthType fromString(String authType) {
    if (authType == null) throw new IllegalArgumentException("type must be non-null");

    // force UNPARSEABLE if by chance UNKNOWN_RESERVED_AUTH_TYPE's text has been used
    if (authType.equals(UNKNOWN_RESERVED_AUTH_TYPE.str)) {
      return UNPARSEABLE_AUTH_TYPE;
    }

    return TYPES_BY_AUTH_STRING.getOrDefault(authType, UNPARSEABLE_AUTH_TYPE);
  }

  /** @return the byte identifier of the auth type, guaranteed to be positive or zero. */
  public byte getIdentifier() {
    return identifier;
  }

  /**
   * @return the auth type represented as a {@link String}, which is made of US_ASCII compatible
   *     characters only
   */
  public String getString() {
    return str;
  }

  /** @see #getString() */
  @Override
  public String toString() {
    return str;
  }
}
