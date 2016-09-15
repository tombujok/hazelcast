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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Versioned;

import java.io.IOException;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;

public class SetOperationOptimised extends BasePutOperation implements IdentifiedDataSerializable, Versioned {

    private boolean newRecord;
    private boolean enableSuperOptimisationTrickery;

    public SetOperationOptimised() {
    }

    public SetOperationOptimised(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
        this.enableSuperOptimisationTrickery = true;
    }

    @Override
    public void afterRun() {
        eventType = newRecord ? ADDED : UPDATED;

        super.afterRun();
    }

    @Override
    public void run() {
        System.err.println("Running NEW set operation enableSuperOptimisationTrickery = " + enableSuperOptimisationTrickery);
        newRecord = recordStore.set(dataKey, dataValue, ttl);
    }

    @Override
    public Object getResponse() {
        return newRecord;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(enableSuperOptimisationTrickery);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        enableSuperOptimisationTrickery = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    // it shares the same ID with he SetOperation
    public int getId() {
        return MapDataSerializerHook.SET;
    }
}
