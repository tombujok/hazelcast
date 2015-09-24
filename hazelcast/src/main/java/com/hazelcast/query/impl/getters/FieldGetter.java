/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.query.impl.getters;


import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class FieldGetter extends Getter {
    private static final int ALL_POSITIONS = -1;

    private final Field field;
    private final int position;
    private final boolean isArray;
    private final Class resultType;


    public FieldGetter(Getter parent, Field field) {
        this(parent, field, null);
    }

    public FieldGetter(Getter parent, Field field, String reducer) {
        super(parent);
        this.field = field;
        this.isArray = field.getType().isArray();
        this.resultType = isArray ? field.getType().getComponentType() : field.getType();

        if (reducer != null) {
            if (!field.getType().isArray()) {
                throw new IllegalStateException("Reducer is allowed only when extracting from arrays");
            }
            String stringValue = reducer.substring(1, reducer.length() - 1);
            position = Integer.parseInt(stringValue);
        } else {
            position = ALL_POSITIONS;
        }
    }

    @Override
    Object getValue(Object obj) throws Exception {
        Object currentObject = getCurrentObject(obj);
        if (currentObject == null) {
            return null;
        }
        if (currentObject instanceof Object[]) {
            return extractFromArray((Object[]) currentObject);
        }
        return extractFromSingleObject(currentObject);
    }

    private Object extractFromArray(Object[] currentArray) throws IllegalAccessException {
        List<Object> result = new ArrayList<Object>();
        for (Object o : currentArray) {
            Object fieldObject = field.get(o);
            if (fieldObject != null) {
                if (isArray) {
                    Object[] fieldArray = (Object[]) fieldObject;
                    if (position == ALL_POSITIONS) {
                        for (Object current : fieldArray) {
                            if (current != null) {
                                result.add(current);
                            }
                        }
                    } else {
                        if (fieldArray.length > position) {
                            result.add(fieldArray[position]);
                        }
                    }
                } else {
                    result.add(fieldObject);
                }
            }
        }
        return result.toArray(new Object[result.size()]);
    }

    private Object extractFromSingleObject(Object currentObject) throws IllegalAccessException {
        Object o = field.get(currentObject);
        if (isArray) {
            if (o == null) {
                return null;
            }
            Object[] array = (Object[]) o;
            if (position == ALL_POSITIONS) {
                return array;
            } else if (array.length > position) {
                return array[position];
            } else {
                return null;
            }
        } else {
            return o;
        }
    }

    private Object getCurrentObject(Object obj) throws Exception {
        return parent != null ? parent.getValue(obj) : obj;
    }

    @Override
    Class getReturnType() {
        return resultType;
    }

    @Override
    boolean isCacheable() {
        return ReflectionHelper.THIS_CL.equals(field.getDeclaringClass().getClassLoader());
    }

    @Override
    public String toString() {
        return "FieldGetter [parent=" + parent + ", field=" + field + "]";
    }
}
