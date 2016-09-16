package com.hazelcast.map;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.version.Version;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
//@Category({QuickTest.class})
public class MapClearVersionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void setClusterVersion_twoNodes_emulatedLowerNode() {
        setLoggingLog4j();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = createInstance(factory, "3.9.0");
        HazelcastInstance instance2 = createInstance(factory, "3.9.0");

        instance1.getCluster().changeClusterVersion(Version.of("3.8"));
        assertClusterVersion(instance1.getCluster(), "3.8");
        assertClusterVersion(instance2.getCluster(), "3.8");

        IMap<String, String> map = instance1.getMap("map");

        map.put("000", "Tom Bujok");

        String doubleoseven = "";

        for (int i = 1; i < 10; i++) {
//            assertEquals("James Bond", map.get("007"));
//            System.err.println("Getting 007 : " + instance1.getCluster().getClusterVersion());
            map.clear();
            sleepAtLeastMillis(100);
            if (i == 3) {
                System.err.println("Changing cluster version to 3.9");
                instance1.getCluster().changeClusterVersion(Version.of("3.9"));
            }
        }

//        assertEquals("James Bond", map.get("007"));

        assertClusterVersion(instance1.getCluster(), "3.9");
        assertClusterVersion(instance2.getCluster(), "3.9");

        map.clear();
//        System.err.println("Cluster version is 3.9");

//        assertEquals("James Bond", map.get("007"));


    }

    private Cluster createSingleInstance(String version) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = createInstance(factory, version);
        return instance.getCluster();
    }

    private static void assertClusterVersion(final Cluster cluster, String version) {
        assertEquals(cluster.getClusterVersion(), Version.of(version));
    }

    HazelcastInstance createInstance(TestHazelcastInstanceFactory factory, String requestedVersion) {
        String currentVersion = System.getProperty("hazelcast.version");
        System.setProperty("hazelcast.version", requestedVersion);
        HazelcastInstanceProxy instance = (HazelcastInstanceProxy) factory.newHazelcastInstance();
        if (currentVersion != null) {
            System.setProperty("hazelcast.version", currentVersion);
        } else {
            System.clearProperty("hazelcast.version");
        }
        return instance;
    }

}
