package com.hazelcast.query.impl.getters;


import java.lang.reflect.Array;
import java.lang.reflect.Field;

public class MultiValueFieldGetter extends Getter implements MultiValueGetter {

    private final Field field;
    private final Class returnType;

    MultiValueFieldGetter(Getter parent, Field field) {
        super(parent);
        this.field = field;
        Class<?> type = field.getType();
        if (type.isArray()) {
            returnType = type.getComponentType();
        } else {
            returnType = type;
        }
    }


    @Override
    Object getValue(Object obj) throws Exception {
        Object currentObject = getCurrentObject(obj);
        if (currentObject == null) {
            return null;
        }
        if (currentObject.getClass().isArray()) {
            return extractFromInputArray(currentObject);
        } else {
            return field.get(currentObject);
        }
    }

    private Object extractFromInputArray(Object inputArray) throws IllegalAccessException {
        Class<?> type = field.getType();
        if (type.isArray()) {
            return extractFromInputArrayWhenCurrentFieldIsArray(inputArray, type.getComponentType());
        } else {
            return extractFromInputArrayWhenCurrentFieldIsNotArray(inputArray, type);
        }
    }

    private Object extractFromInputArrayWhenCurrentFieldIsNotArray(Object inputArray, Class<?> type) throws IllegalAccessException {
        int length = Array.getLength(inputArray);
        Object dst = Array.newInstance(type, length);
        for (int i = 0; i < length; i ++) {
            Object currentObject = Array.get(inputArray, i);
            Object value = field.get(currentObject);
            Array.set(dst, i, value);
        }
        return dst;
    }

    private Object extractFromInputArrayWhenCurrentFieldIsArray(Object inputArray, Class<?> type) throws IllegalAccessException {
        int resultLength = getSize(inputArray);
        int inputArrayLength = Array.getLength(inputArray);
        Object dst = Array.newInstance(type, resultLength);
        int dstIndex = 0;
        for (int i = 0; i < inputArrayLength; i ++) {
            Object currentObject = Array.get(inputArray, i);
            if (currentObject == null) {
                continue;
            }
            Object innerArray = field.get(currentObject);
            if (innerArray == null) {
                continue;
            }
            int innerArrayLength = Array.getLength(innerArray);
            for (int j = 0; j < innerArrayLength; j++) {
                Object value = Array.get(innerArray, j);
                Array.set(dst, dstIndex, value);
                dstIndex++;
            }
        }
        return dst;
    }

    private int getSize(Object col) throws IllegalAccessException {
        int size = 0;
        int length = Array.getLength(col);
        for (int i = 0; i < length; i ++) {
            Object o = Array.get(col, i);
            if (o != null) {
                Object fieldValue = field.get(o);
                if (fieldValue != null) {
                    size += Array.getLength(fieldValue);
                }
            }
        }
        return size;
    }

    private Object getCurrentObject(Object obj) throws Exception {
        return parent != null ? parent.getValue(obj) : obj;
    }

    @Override
    Class getReturnType() {
        return returnType;
    }

    @Override
    boolean isCacheable() {
        return ReflectionHelper.THIS_CL.equals(field.getDeclaringClass().getClassLoader());
    }

}
