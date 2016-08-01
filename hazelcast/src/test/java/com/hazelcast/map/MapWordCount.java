package com.hazelcast.map;

import com.hazelcast.aggregation.EntryAggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.UuidUtil;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

public class MapWordCount extends HazelcastTestSupport {

    private static final String[] DATA_RESOURCES_TO_LOAD =
            {"ulysses.txt", "warandpeace.txt", "anna_karenina.txt", "dracula.txt"};
//                    , "ulysses.txt", "warandpeace.txt", "anna_karenina.txt", "dracula.txt",
//                    "ulysses.txt", "warandpeace.txt", "anna_karenina.txt", "dracula.txt", "ulysses.txt", "warandpeace.txt", "anna_karenina.txt", "dracula.txt"};

    private static final String MAP_NAME = "articles";

    public static void main(String[] args)
            throws Exception {

        // Prepare Hazelcast cluster
        HazelcastInstance hazelcastInstance = buildCluster(3);

        try {
            // Read data
            System.out.println("Filling map...");
            for (int i = 0; i < 100; i++) {
                fillMapWithDataEachLineNewEntry(hazelcastInstance);
//                fillMapWithData(hazelcastInstance);
            }
            IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);


            System.out.println("Garbage collecting...");
            for(int i = 0 ; i < 10 ; i++) {
                System.gc();
            }

            for(int i = 0 ; i < 10 ; i++) {
                System.out.println("Executing job...");
                long start = System.currentTimeMillis();

                Map<String, AtomicInteger> result = map.values(new WordCountAggregator());

//            TreeSet<Map.Entry<String, MutableInt>> resultSorted = new TreeSet<Map.Entry<String, MutableInt>>(new Comparator<Map.Entry<String, MutableInt>>() {
//                @Override
//                public int compare(Map.Entry<String, MutableInt> o1, Map.Entry<String, MutableInt> o2) {
//                    int comparisionId = compareInts(o2.getValue().value, o1.getValue().value);
//                    if (comparisionId == 0) {
//                        return o1.getKey().compareTo(o2.getKey());
//                    }
//                    return comparisionId;
//                }
//            });
//            for (Map.Entry entry : result.entrySet()) {
//                resultSorted.add(entry);
//            }

                System.err.println(result.size());
                System.err.println("TimeTaken=" + (System.currentTimeMillis() - start));
                System.err.println("---------------------------------------------");
                System.gc();
            }


        } finally {
            // Shutdown cluster
            Hazelcast.shutdownAll();
        }
    }

    public static int compareInts(int x, int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    public static HazelcastInstance buildCluster(int memberCount) {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        networkConfig.getJoin().getTcpIpConfig().setMembers(Arrays.asList(new String[]{"127.0.0.1"}));

        MapConfig mapConfig = new MapConfig();
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        mapConfig.setName(MAP_NAME);
        mapConfig.setBackupCount(0);
        config.addMapConfig(mapConfig);

        config.setProperty("hazelcast.query.predicate.parallel.evaluation", "true");
        config.setProperty("hazelcast.logging.type", "log4j");


        HazelcastInstance[] hazelcastInstances = new HazelcastInstance[memberCount];
        for (int i = 0; i < memberCount; i++) {
            hazelcastInstances[i] = Hazelcast.newHazelcastInstance(config);
        }
        return hazelcastInstances[0];
    }

    private static void fillMapWithData(HazelcastInstance hazelcastInstance)
            throws Exception {

        IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);
        for (String file : DATA_RESOURCES_TO_LOAD) {
            InputStream is = MapWordCount.class.getResourceAsStream("/wordcount/" + file);
            LineNumberReader reader = new LineNumberReader(new InputStreamReader(is));

            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            map.put(UuidUtil.newSecureUuidString(), sb.toString());

            is.close();
            reader.close();
        }
    }

    private static void fillMapWithDataEachLineNewEntry(HazelcastInstance hazelcastInstance)
            throws Exception {

        IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);
        for (String file : DATA_RESOURCES_TO_LOAD) {
            InputStream is = MapWordCount.class.getResourceAsStream("/wordcount/" + file);
            LineNumberReader reader = new LineNumberReader(new InputStreamReader(is));

            int batchSize = 10000;
            int batchSizeCount = 0;
            Map batch = new HashMap(batchSize);
            String line = null;
            while ((line = reader.readLine()) != null) {
                batch.put(UuidUtil.newSecureUuidString(), line);
                batchSizeCount++;
                if(batchSizeCount == batchSize) {
                    map.putAll(batch);
                    batchSizeCount =0;
                    batch.clear();

                }
            }

            if(batchSizeCount > 0) {
                map.putAll(batch);
                batch.clear();
            }

            is.close();
            reader.close();
        }
    }


    private static class MutableInt implements Serializable {
        private int value = 0;

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    public static String cleanWord(String word) {
        return word.replaceAll("[^A-Za-z0-9]", "");
    }

    private static class WordCountAggregator implements EntryAggregator<Map<String, MutableInt>, String, String> {

        Map<String, MutableInt> result = new HashMap<String, MutableInt>(1000);

        @Override
        public void accumulate(Map.Entry<String, String> entry) {
            accumulate(entry.getValue(), 1);
        }

        public void accumulate(String value, int times) {
            StringTokenizer tokenizer = new StringTokenizer(value);

            while (tokenizer.hasMoreTokens()) {
                String word = cleanWord(tokenizer.nextToken()).toLowerCase();

                MutableInt count = result.get(word);
                if (count == null) {
                    count = new MutableInt();
                    result.put(word, count);
                }
                count.value += times;
            }
        }

        @Override
        public void accumulate(Collection<Map.Entry<String, String>> entries) {
            for (Map.Entry<String, String> entry : entries) {
                accumulate(entry.getValue(), 1);
            }
        }

        @Override
        public void combine(Collection<EntryAggregator> aggregators) {
            for (EntryAggregator aggregator : aggregators) {
                combine(aggregator);
            }
        }

        @Override
        public void combine(EntryAggregator aggregator) {
            WordCountAggregator aggr = (WordCountAggregator) aggregator;
            for (Map.Entry<String, MutableInt> toCombine : aggr.result.entrySet()) {
                doCombine(toCombine);
            }
        }

        public void doCombine(Map.Entry<String, MutableInt> toCombine) {
            String word = toCombine.getKey();
            MutableInt count = result.get(word);
            if (count == null) {
                count = new MutableInt();
                result.put(word, count);
            }
            count.value += toCombine.getValue().value;
        }

        @Override
        public Map<String, MutableInt> aggregate() {
            return result;
        }

    }

}
