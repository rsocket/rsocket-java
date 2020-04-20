package io.rsocket.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/** JDK 8 Static Final Fields access modifier Only used for test purpose */
public class FieldHelper {

  static void makeNonFinal(Field field) throws NoSuchFieldException, IllegalAccessException {
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
  }

  public static void setFinalStatic(Field field, Object newValue) throws Exception {
    makeNonFinal(field);
    field.setAccessible(true);
    field.set(null, newValue);
  }
}
