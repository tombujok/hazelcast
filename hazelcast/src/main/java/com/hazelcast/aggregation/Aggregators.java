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

import com.hazelcast.aggregation.impl.DoubleAverageAggregator;

public final class Aggregators {

    private Aggregators() {
    }

    public static <K, V> EntryAggregator<Double, K, V> doubleAverage() {
        return new DoubleAverageAggregator(null);
    }

    public static <K, V> EntryAggregator<Double, K, V> doubleAverage(String attributePath) {
        return new DoubleAverageAggregator(attributePath);
    }

}
