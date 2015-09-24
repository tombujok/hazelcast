package com.hazelcast.query.impl.getters;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class GetterFactory {
    public static Getter newFieldGetter(Getter parent, Field field, String reducerSuffix) {
        return new FieldGetter(parent, field, reducerSuffix);
    }

    public static Getter newMethodGetter(Getter parent, Method method) {
        return new MethodGetter(parent, method);
    }

    public static Getter newThisGetter(Getter parent, Object object) {
        return new ThisGetter(parent, object);
    }

}
