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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Versioned;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

public class ClearOperation extends MapOperation implements BackupAwareOperation,
        PartitionAwareOperation, MutatingOperation, Versioned {

    // just an example that a DataSerializable & Versioned object may be included and it will have its own version
    private ClearBackupOperation op;

    private boolean shouldBackup;
    private int numberOfClearedEntries;

    public ClearOperation() {
    }

    public ClearOperation(String name) {
        super(name);
        createRecordStoreOnDemand = false;
        this.op = new ClearBackupOperation();
    }

    @Override
    public void run() {
        System.err.println("Running ClearOperation - (extraField != null) ==" + (op != null));

        // near-cache clear will be called multiple times by each clear operation,
        // but it's still preferred to send a separate operation to clear near-cache.
        clearLocalNearCache();

        if (recordStore == null) {
            return;
        }

        numberOfClearedEntries = recordStore.clear();
        shouldBackup = true;
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        hintMapEvent();
    }

    private void hintMapEvent() {
        mapEventPublisher.hintMapEvent(getCallerAddress(), name, EntryEventType.CLEAR_ALL,
                numberOfClearedEntries, getPartitionId());
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    @Override
    public int getSyncBackupCount() {
        return mapServiceContext.getMapContainer(name).getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return mapServiceContext.getMapContainer(name).getAsyncBackupCount();
    }

    @Override
    public Object getResponse() {
        return numberOfClearedEntries;
    }

    public Operation getBackupOperation() {
        ClearBackupOperation clearBackupOperation = new ClearBackupOperation(name);
        clearBackupOperation.setServiceName(SERVICE_NAME);
        return clearBackupOperation;
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


}
