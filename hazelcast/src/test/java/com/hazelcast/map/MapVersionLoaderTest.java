package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.version.Version;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
//@Category({QuickTest.class})
public class MapVersionLoaderTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testLoading_380() {
        HazelcastInstance instance = createSingleInstance("3.8.0");
        IMap<String, String> map = instance.getMap("loader");

        assertEquals("Chicago", map.get("ORD"));
    }

    @Test
    public void testLoading_390() {
        HazelcastInstance instance = createSingleInstance("3.9.0");
        IMap<String, String> map = instance.getMap("loader");

        assertEquals("Chicago", map.get("ORD"));
    }

    @Test(expected = IllegalStateException.class)
    public void testTryLoadingOn38() {
        HazelcastInstance instance = createSingleInstance("3.8.0");
        IMap<String, String> map = instance.getMap("loader");

        assertTrue(map.tryLoadAll(true));
    }

    @Test
    public void testTryLoadingOn39() {
        HazelcastInstance instance = createSingleInstance("3.9.0");
        IMap<String, String> map = instance.getMap("loader");

        System.err.println("Try load all");
        assertTrue(map.tryLoadAll(true));
        assertEquals("Chicago", map.get("ORD"));
    }

    private HazelcastInstance createSingleInstance(String version) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);

        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("loader");

        MapStoreConfig storeConfig = new MapStoreConfig();
        storeConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        storeConfig.setEnabled(true);
        storeConfig.setImplementation(new MapLoader<String, String>() {
            @Override
            public String load(String key) {
                if (key.equals("ORD")) {
                    return "Chicago";
                } else if (key.equals("JFK")) {
                    return "New York";
                }
                return null;
            }

            @Override
            public Map<String, String> loadAll(Collection<String> keys) {
                Map<String, String> result = new HashMap<String, String>();
                if (keys.contains("ORD")) {
                    result.put("ORD", "Chicago");
                }
                if (keys.contains("JFK")) {
                    result.put("JFK", "New York");
                }
                return result;
            }

            @Override
            public Iterable<String> loadAllKeys() {
                List<String> result = new ArrayList<String>();
                result.add("ORD");
                result.add("JFK");
                return result;
            }
        });

        mapConfig.setMapStoreConfig(storeConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance instance = createInstance(factory, version, config);
        return instance;
    }

    private static void assertClusterVersion(final Cluster cluster, String version) {
        assertEquals(cluster.getClusterVersion(), Version.of(version));
    }

    HazelcastInstance createInstance(TestHazelcastInstanceFactory factory, String requestedVersion, Config config) {
        String currentVersion = System.getProperty("hazelcast.version");
        System.setProperty("hazelcast.version", requestedVersion);
        HazelcastInstanceProxy instance = (HazelcastInstanceProxy) factory.newHazelcastInstance(config);
        if (currentVersion != null) {
            System.setProperty("hazelcast.version", currentVersion);
        } else {
            System.clearProperty("hazelcast.version");
        }
        return instance;
    }

}
