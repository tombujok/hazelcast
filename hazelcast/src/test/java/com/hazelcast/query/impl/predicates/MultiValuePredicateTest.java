package com.hazelcast.query.impl.predicates;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.aggregation.impl.ComparableMaxAggregation;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestCollectionUtils;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.hazelcast.test.TestCollectionUtils.setOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyCollectionOf;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MultiValuePredicateTest extends HazelcastTestSupport {


    @Test
    public void testAndPredicate_() throws Exception {
        final HazelcastInstance instance = createHazelcastInstance();
        final IMap<Integer, Body> map = instance.getMap("map");
        map.addIndex("limbs(0).name", false);
        map.addIndex("limbs.name", true);

        Body body1 = new Body("body1",
                new Limb("ugly leg",
                        new Nail("red"),
                        new Nail("blue")
                ),
                new Limb("missing hand",
                        new Nail("yellow")
                )
        );


        Body body2 = new Body("body2",
                new Limb("hook",
                        new Nail("yellow"),
                        new Nail("red")
                ),
                new Limb("ugly leg")
        );

        Body body3 = new Body("body3");
        body3.insertStuffIntoPocket("left pocket", "beer");

        map.put(1, body1);
        map.put(2, body2);
        map.put(3, body3);

        Predicate predicate = new ContainsPredicate("limbs.nails.colour", "red");
        Collection<Body> values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1, body2));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new ContainsPredicate("pockets.values.toArray", "beer");
        values = map.values(predicate);
        assertThat(values, contains(body3));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new ContainsPredicate("limbs.name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1, body2));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs(0).name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs(4).name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, empty());
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");


        predicate = new EqualPredicate("limbs(1).name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, contains(body2));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");


        predicate = new ContainsPredicate("limbs(0).nails.colour", "yellow");
        values = map.values(predicate);
        assertThat(values, contains(body2));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs(0).name", null);
        values = map.values(predicate);
        assertThat(values, contains(body3));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new ContainsAllPredicate("limbs.nails.colour", setOf((Comparable)"red"));
        values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1, body2));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");


        predicate = new ContainsAllPredicate("limbs.nails.colour", setOf((Comparable) "red", "blue"));
        values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

    }

    private static class Body implements Serializable {
        private final String name;
        private Limb[] limbs;
        private Map<String, String> pockets;

        public Body(String name, Limb...limbs) {
            this.pockets = new HashMap<String, String>();
            this.name = name;
            this.limbs = limbs;
        }

        public void insertStuffIntoPocket(String pocket, String stuff) {
            pockets.put(pocket, stuff);
        }

        @Override
        public String toString() {
            return "Body{" +
                    "name='" + name + '\'' +
                    ", limbs=" + Arrays.toString(limbs) +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Body body = (Body) o;

            if (!name.equals(body.name)) return false;
            if (!Arrays.equals(limbs, body.limbs)) return false;
            return pockets.equals(body.pockets);

        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + Arrays.hashCode(limbs);
            result = 31 * result + pockets.hashCode();
            return result;
        }
    }

    private static class Limb implements Serializable {
        private final String name;
        private final Nail[] nails;

        public Limb(String name, Nail...nails) {
            this.name = name;
            this.nails = nails;
        }

        @Override
        public String toString() {
            return "Limb{" +
                    "name='" + name + '\'' +
                    ", nails=" + Arrays.toString(nails) +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Limb limb = (Limb) o;

            if (!name.equals(limb.name)) return false;
            return Arrays.equals(nails, limb.nails);

        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + Arrays.hashCode(nails);
            return result;
        }
    }

    private static class Nail implements Serializable {
        private final String colour;
        public Nail(String colour) {
            this.colour = colour;
        }

        @Override
        public String toString() {
            return "Nail{" +
                    "color='" + colour + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Nail nail = (Nail) o;

            return colour.equals(nail.colour);

        }

        @Override
        public int hashCode() {
            return colour.hashCode();
        }
    }
}
