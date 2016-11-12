package ducktyping;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DuckTypeUtil {

  public static <T> T newInstance(Class<T> type, Class<?>[] parameterTypes, Object[] args) throws Exception {
    Constructor<T> constructor = getConstructor(type, parameterTypes);
    constructor.setAccessible(true);
    return constructor.newInstance(args);
  }

  private static <T> Constructor<T> getConstructor(Class<T> type, Class<?>[] parameterTypes) throws Exception {
    try {
      return type.getConstructor(parameterTypes);
    } catch (NoSuchMethodException e1) {
      try {
        return type.getDeclaredConstructor(parameterTypes);
      } catch (NoSuchMethodException e2) {
        throw e2;
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T wrap(Class<T> duckType, Object o) {
    return (T) Proxy.newProxyInstance(duckType.getClassLoader(), new Class<?>[] { duckType }, getHandler(o));
  }

  private static InvocationHandler getHandler(Object o) {
    return new InvocationHandler() {
      private Map<Method, Method> methods = new ConcurrentHashMap<>();

      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Method meth = getMethod(method, o);
        try {
          return meth.invoke(o, args);
        } catch (InvocationTargetException e) {
          throw e.getTargetException();
        }
      }

      private Method getMethod(Method method, Object o) throws Exception {
        Method realMethod = methods.get(method);
        if (realMethod == null) {
          realMethod = getMethod(method, o, o.getClass());
          realMethod.setAccessible(true);
          methods.put(method, realMethod);
        }
        return realMethod;
      }

      private Method getMethod(Method method, Object o, Class<? extends Object> clazz) throws Exception {
        try {
          return clazz.getMethod(method.getName(), method.getParameterTypes());
        } catch (NoSuchMethodException e1) {
          try {
            return clazz.getDeclaredMethod(method.getName(), method.getParameterTypes());
          } catch (NoSuchMethodException e2) {
            if (clazz == Object.class) {
              throw e2;
            }
            return getMethod(method, o, clazz.getSuperclass());
          }
        }
      }
    };
  }

}
