package io.rsocket.test.extension;

import io.rsocket.Closeable;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.ReflectionUtils;

public abstract class AbstractExtension implements ParameterResolver, AfterTestExecutionCallback {
  protected abstract Class<?> type();

  protected Object newInstance() {
    return ReflectionUtils.newInstance(type());
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().isAssignableFrom(type());
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    try {
      Object instance = newInstance();
      put(extensionContext, instance);
      return instance;
    } catch (Exception e) {
      throw new ParameterResolutionException("Unable to create external resource", e);
    }
  }

  @Override
  public void afterTestExecution(ExtensionContext context) {
    Closeable closeable = closeable(remove(context));
    if (closeable != null) {
      closeable.close().block();
    }
  }

  private Closeable closeable(Object instance) {
    return instance instanceof Closeable ? (Closeable) instance : null;
  }

  private Store store(ExtensionContext context) {
    return context.getStore(Namespace.create(getClass(), context));
  }

  private void put(ExtensionContext context, Object value) {
    store(context).put(context.getRequiredTestMethod(), value);
  }

  private Object remove(ExtensionContext context) {
    return store(context).remove(context.getRequiredTestMethod());
  }
}
