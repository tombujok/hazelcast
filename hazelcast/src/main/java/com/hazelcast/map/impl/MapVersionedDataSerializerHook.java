/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayVersionedDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.operation.PutOperation;
import com.hazelcast.nio.serialization.VersionedDataSerializableFactory;
import com.hazelcast.nio.serialization.Versioned;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MAP_VERSIONED_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.MAP_VERSIONED_DS_FACTORY_ID;

public final class MapVersionedDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(MAP_VERSIONED_DS_FACTORY, MAP_VERSIONED_DS_FACTORY_ID);

    public static final int PUT = 0;

    private static final int LEN = PUT + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public VersionedDataSerializableFactory createFactory() {
        ConstructorFunction<Integer, Versioned>[] constructors = new ConstructorFunction[LEN];

        constructors[PUT] = new ConstructorFunction<Integer, Versioned>() {
            public Versioned createNew(Integer arg) {
                return new PutOperation();
            }
        };

        return new ArrayVersionedDataSerializableFactory(constructors);
    }
}
