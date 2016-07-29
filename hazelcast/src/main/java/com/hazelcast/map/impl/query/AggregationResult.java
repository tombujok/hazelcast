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

package com.hazelcast.map.impl.query;

import com.hazelcast.aggregation.EntryAggregator;

import java.io.Serializable;
import java.util.Collection;

/**
 * Contains the result of an aggregation evaluation.
 */
public class AggregationResult implements Serializable {

    private EntryAggregator aggregator;
    private Collection<Integer> partitionIds;

    public AggregationResult(EntryAggregator aggregator) {
        this.aggregator = aggregator;
    }

    public EntryAggregator getAggregator() {
        return aggregator;
    }

    public Collection<Integer> getPartitionIds() {
        return partitionIds;
    }

    public void setPartitionIds(Collection<Integer> partitionIds) {
        this.partitionIds = partitionIds;
    }

}
