package com.hazelcast.map;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapAggregationTest extends HazelcastTestSupport {

    @Test
    public void doubleAvgWith1Node() {
        IMap<String, Double> map = getMapWithNodeCount(1);
        map.put("key1", 1.0d);
        map.put("key2", 4.0d);
        map.put("key3", 7.0d);

        Double avg = map.values(Aggregators.doubleAverage());
        assertEquals(Double.valueOf(4.0d), avg);
    }

    @Test
    public void doubleAvgWith3Nodes() {
        IMap<String, Double> map = getMapWithNodeCount(3);
        map.put("key1", 1.0d);
        map.put("key2", 4.0d);
        map.put("key3", 7.0d);

        Double avg = map.values(Aggregators.doubleAverage());
        assertEquals(Double.valueOf(4.0d), avg);
    }

    @Test
    public void double_1millionFalues_AvgWith3Nodes() {
        IMap<Long, Double> map = getMapWithNodeCount(1);
        System.err.println("Initialising");

        int elementCount = 10000000;
        double value = 0;
        Map<Long, Double> values = new HashMap<Long, Double>(elementCount);
        for (long i = 0L; i < elementCount; i++) {
            values.put(i, value++);
        }

        System.err.println("Putting");
        map.putAll(values);


        System.err.println("Executing bare metal");
        long start1 = System.currentTimeMillis();

        int count = 0;
        double sum = 0d;
        for (Double d : values.values()) {
            sum += d;
            count++;
        }
        Double avg1 = sum / ((double) count);
        long stop1 = System.currentTimeMillis();
        System.err.println("Finished avg in " + (stop1 - start1) + " millis avg=" + avg1);

        System.err.println("Executing");
        long start = System.currentTimeMillis();
        Double avg = map.values(Aggregators.doubleAverage());
        long stop = System.currentTimeMillis();
        System.err.println("Finished avg in " + (stop - start) + " millis avg=" + avg);
        System.err.flush();
    }


    private <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);


        Config config = new Config();
        config.setProperty("hazelcast.partition.count", "3");
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("aggr");
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        config.addMapConfig(mapConfig);

        HazelcastInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
    }
}
