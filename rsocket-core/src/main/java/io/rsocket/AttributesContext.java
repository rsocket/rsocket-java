package io.rsocket;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Attributes Context
 *
 * @author linux_china
 */
public interface AttributesContext {

  /**
   * Return a mutable map of attributes for the current context.
   *
   * @return current attributes.
   */
  Map<String, Object> getAttributes();

  /**
   * Return the request attribute value if present.
   *
   * @param name the attribute name
   * @param <T> the attribute type
   * @return the attribute value
   */
  @SuppressWarnings("unchecked")
  @Nullable
  default <T> T getAttribute(String name) {
    return (T) getAttributes().get(name);
  }

  default void setAttribute(String name, @Nonnull Object value) {
    getAttributes().put(name, value);
  }

  /**
   * Return the attribute value, or a default, fallback value.
   *
   * @param name the attribute name
   * @param defaultValue a default value to return instead
   * @param <T> the attribute type
   * @return the attribute value
   */
  @SuppressWarnings("unchecked")
  default <T> T getAttributeOrDefault(String name, T defaultValue) {
    return (T) getAttributes().getOrDefault(name, defaultValue);
  }
}
