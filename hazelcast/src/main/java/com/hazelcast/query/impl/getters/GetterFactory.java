package com.hazelcast.query.impl.getters;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;

public class GetterFactory {
    public static Getter newFieldGetter(Getter parent, Field field, String reducerSuffix) {
        Class<?> type = field.getType();
        if (parent instanceof MultiValueGetter || type.isArray()) {
            Getter getter = new MultiValueFieldGetter(parent, field);
            if (reducerSuffix == null) {
                return getter;
            }
            return new ReducingGetter(getter, reducerSuffix);
        } else {
            if (reducerSuffix != null) {
                throw new IllegalStateException("ReducerGetter require multi value input.");
            }
        }
        return new FieldGetter(parent, field);
    }

    public static Getter newMethodGetter(Getter parent, Method method) {
        return new MethodGetter(parent, method);
    }

    public static Getter newThisGetter(Getter parent, Object object) {
        return new ThisGetter(parent, object);
    }

}
