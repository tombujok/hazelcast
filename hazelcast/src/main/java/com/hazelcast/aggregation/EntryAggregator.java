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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * Base class for all aggregators. Exposes API for parallel two-phase aggregations:
 * - accumulation of entries by multiple instance of aggregators
 * - combining all aggregators into one to calculate the final result
 * <p>
 * EntryAggregator does not have to be thread-safe.
 * accumulate() and combine() calls may be interwoven.
 *
 * @param <R> aggregation result
 * @param <K> entry key type
 * @param <V> entry value type
 */
public interface EntryAggregator<K, V, R, A extends EntryAggregator<K, V, R, A >> extends Serializable {

    /**
     * Accumulates t
     *
     * @param entries entries to accumulate.
     */
    void accumulate(Collection<Map.Entry<K, V>> entries);

    /**
     * @param aggregator aggregator providing intermediary results to be combined into the results of this aggregator.
     */
    void combine(A aggregator);

    /**
     * Returns the result of the aggregation. The result may be calculated in this call or cached by the aggregator.
     *
     * @return returns the result of the aggregation.
     */
    R aggregate();

}
