package io.rsocket.reflection;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/** JDK 9 Static Final fields access modifier Only used for test purpose */
public class FieldHelper {

  private static final VarHandle MODIFIERS;

  static {
    try {
      var lookup = MethodHandles.privateLookupIn(Field.class, MethodHandles.lookup());
      MODIFIERS = lookup.findVarHandle(Field.class, "modifiers", int.class);
    } catch (IllegalAccessException | NoSuchFieldException ex) {
      throw new RuntimeException(ex);
    }
  }

  static void makeNonFinal(Field field) {
    int mods = field.getModifiers();
    if (Modifier.isFinal(mods)) {
      MODIFIERS.set(field, mods & ~Modifier.FINAL);
    }
  }

  public static void setFinalStatic(Field field, Object newValue) throws Exception {
    makeNonFinal(field);
    field.setAccessible(true);
    field.set(null, newValue);
  }
}
