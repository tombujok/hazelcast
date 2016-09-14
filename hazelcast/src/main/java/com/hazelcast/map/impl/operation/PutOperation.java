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
import com.hazelcast.map.impl.MapVersionedDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.Versioned;

import java.io.IOException;

public class PutOperation extends BasePutOperation implements IdentifiedDataSerializable, Versioned {

    private PutOperation op;

    public PutOperation() {
    }

    public PutOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
        this.op = new PutOperation();
    }

    @Override
    public void run() {
        dataOldValue = mapServiceContext.toData(recordStore.put(dataKey, dataValue, ttl));
    }

    @Override
    public Object getResponse() {
        return dataOldValue;
    }

    @Override
    public int getFactoryId() {
        return (short) MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.PUT;
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        if (in.getVersion().isVersion(3, 9)) {
            op = in.readObject();
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (out.getVersion().isVersion(3, 9)) {
            out.writeObject(op);
        }
    }

    // assumptions
    // -> 3.8 will never send/receive version 3.8 if cluster version is 3.7
    // -> 3.8 may send/receive version 3.7 if cluster version is 3.8 (since other operations may be in-flight)

}
