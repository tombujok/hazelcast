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

package com.hazelcast.aggregation;

import com.hazelcast.query.impl.Extractable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * Base class for all aggregators. Exposes API for parallel two-phase aggregations:
 * - accumulation of entries by multiple instance of aggregators
 * - combining all aggregators to calculate the final result
 * <p>
 * accumulate() and combine() calls may be interwoven.
 *
 * @param <R> aggregation result
 * @param <K> entry key type
 * @param <V> entry value type
 */
public interface EntryAggregator<R, K, V> extends Serializable {

    /**
     * @param entry entry to accumulate.
     */
    public abstract void accumulate(Map.Entry<K, V> entry);

    /**
     * @param entries entries to accumulate.
     */
    public abstract void accumulate(Collection<Map.Entry<K, V>> entries);

    /**
     * @param aggregator aggregator providing intermediary results to be combined into the results of this aggregator.
     */
    public abstract void combine(EntryAggregator aggregator);

    /**
     * @param aggregators aggregators providing intermediary results to be combined into the results of this aggregator.
     */
    public abstract void combine(Collection<EntryAggregator> aggregators);

    /**
     * Returns the result of the aggregation. The result may be calculated in this call or cached by the aggregator.
     *
     * @return returns the result of the aggregation.
     */
    public abstract R aggregate();

}
