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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class FieldGetter extends Getter {
    private static final int ALL_POSITIONS = -1;

    private final Field field;
    private final int position;
    private final boolean isArray;
    private final boolean isCollection;
    private final Class resultType;

    public FieldGetter(Getter parent, Field field, String reducer, Class resultType) {
        super(parent);
        this.field = field;
        this.isArray = field.getType().isArray();
        this.isCollection = Collection.class.isAssignableFrom(field.getType());
        this.resultType = resultType == null ? (isArray ? field.getType().getComponentType() : field.getType()) : resultType;

        if (reducer != null) {
            if (!isArray && !isCollection) {
                throw new IllegalStateException("Reducer is allowed only when extracting from arrays or collections");
            }
            String stringValue = reducer.substring(1, reducer.length() - 1);
            position = Integer.parseInt(stringValue);
        } else {
            position = ALL_POSITIONS;
        }
    }


    public FieldGetter(Getter parent, Field field, String reducer) {
        this(parent, field, reducer, null);
    }

    @Override
    Object getValue(Object obj) throws Exception {
        Object parentObject = getParentObject(obj);
        if (parentObject == null) {
            return null;
        }
        if (parentObject instanceof Object[]) {
            return extractFromArray((Object[]) parentObject);
        }
        if (parentObject instanceof Collection) {
            return extractFromCollection((Collection) parentObject);
        }
        return extractFromSingleObject(parentObject);
    }

    private Object extractFromCollection(Collection currentCollection) throws IllegalAccessException {
        List<Object> result = new ArrayList<Object>();
        for (Object o : currentCollection) {
            collectResult(result, o);
        }
        return result.toArray(new Object[result.size()]);
    }

    private Object extractFromArray(Object[] currentArray) throws IllegalAccessException {
        List<Object> result = new ArrayList<Object>();
        for (Object o : currentArray) {
            collectResult(result, o);
        }
        return result.toArray(new Object[result.size()]);
    }

    private void collectResult(List<Object> result, Object o) throws IllegalAccessException {
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
            } else if (isCollection) {
                Collection collection = (Collection) fieldObject;
                if (position == ALL_POSITIONS) {
                    result.addAll(collection);
                } else {
                    Object itemAtPosition = getItemAtPosition(collection, position);
                    if (itemAtPosition != null) {
                        result.add(itemAtPosition);
                    }
                }
            } else {
                result.add(fieldObject);
            }
        }
    }

    private Object extractFromSingleObject(Object currentObject) throws IllegalAccessException {
        Object o = field.get(currentObject);
        if (o == null) {
            return null;
        }
        if (isArray) {
            if (position == ALL_POSITIONS) {
                return o;
            }
            Object[] array = (Object[]) o;
            if (array.length > position) {
                return array[position];
            } else {
                return null;
            }
        } else if (isCollection) {
            if (position == ALL_POSITIONS) {
                return o;
            }
            Collection collection = (Collection) o;
            return getItemAtPosition(collection, position);
        } else {
            return o;
        }
    }

    private Object getItemAtPosition(Collection collection, int position) {
        if (collection == null) {
            return null;
        }
        if (collection.size() > position) {
            if (collection instanceof List) {
                return ((List) collection).get(position);
            } else {
                Iterator iterator = collection.iterator();
                Object item = null;
                for (int i = 0; i < position + 1; i++) {
                    item = iterator.next();
                }
                return item;
            }
        }
        return null;
    }

    private Object getParentObject(Object obj) throws Exception {
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
