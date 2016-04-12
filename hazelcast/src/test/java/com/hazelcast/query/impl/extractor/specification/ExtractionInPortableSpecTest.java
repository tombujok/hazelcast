package com.hazelcast.query.impl.extractor.specification;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.impl.extractor.AbstractExtractionTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.NO_INDEX;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.ORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.UNORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.PORTABLE;
import static java.util.Arrays.asList;

/**
 * Specification test that verifies the behavior of corner-cases extraction in single-value attributes.
 * <p/>
 * Extraction mechanism: IN-BUILT REFLECTION EXTRACTION
 * <p/>
 * This test is parametrised on two axes (see the parametrisationData() method):
 * - in memory format
 * - indexing
 */
@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
@Ignore
public class ExtractionInPortableSpecTest extends AbstractExtractionTest {

    public ExtractionInPortableSpecTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    protected Configurator getInstanceConfigurator() {
        return new Configurator() {
            @Override
            public void doWithConfig(Config config, Multivalue mv) {
                config.getSerializationConfig().addPortableFactory(ComplexDataStructure.PersonPortableFactory.ID, new ComplexDataStructure.PersonPortableFactory());
            }
        };
    }

//    @Test
//    public void correct_attribute_name() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("name", "Porsche"), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_nestedAttribute_name() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("engine.power", 300), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableAttribute() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("engine", PORSCHE.engine), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableNestedAttribute() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("engine.chip", PORSCHE.engine.chip), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].name", "front"), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[1].name", "front"), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableAtTheEnd_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chip", ((PortableDataStructure.WheelPortable) PORSCHE.wheels[0]).chip), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableAtTheEnd_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chip", new PortableDataStructure.ChipPortable(123)), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableArrayAtTheEnd_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chips[1]", (PortableDataStructure.ChipPortable) (((PortableDataStructure.WheelPortable) PORSCHE.wheels[0]).chips)[1]), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableArrayAtTheEnd_primitiveAttribute_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chips[0].power", 15), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableArrayAtTheEnd_primitiveAttribute_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chips[0].power", 20), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableArrayAtTheEnd_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chips[0]", (PortableDataStructure.ChipPortable) (((PortableDataStructure.WheelPortable) PORSCHE.wheels[0]).chips)[1]), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_primitiveArrayAtTheEnd_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].serial[1]", 12), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_primitiveArrayAtTheEnd_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].serial[1]", 123), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_portableArrayAtTheEnd_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0]", (PortableDataStructure.WheelPortable) PORSCHE.wheels[0]), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayAtTheEnd_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[1]", (PortableDataStructure.WheelPortable) PORSCHE.wheels[0]), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_primitiveArrayAtTheEnd_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("model[0]", "911"), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_primitiveArrayAtTheEnd_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("model[0]", "956"), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_primitiveArrayAtTheEnd_matchingX() {
//        PortableDataStructure.XPortable x = new PortableDataStructure.XPortable();
//
//        execute(Input.of(x),
//                Query.of(Predicates.equal("chips[0].serial[0]", 41), mv),
//                Expected.of(x));
//    }
//
//    @Test
//    public void correct_primitiveArrayAtTheEnd_notMatchingX() {
//        PortableDataStructure.XPortable x = new PortableDataStructure.XPortable();
//
//        execute(Input.of(x),
//                Query.of(Predicates.equal("chips[0].serial[0]", 10), mv),
//                Expected.empty());
//    }

    // TODO no object on the server classpath -> ClientMapStandaloneTest

//    @Test
//    @Ignore("Does not throw exception for now - inconsistent with other query mechanisms")
//    public void wrong_attribute_name() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("name12312", "Porsche"), mv),
//                Expected.of(QueryException.class));
//    }

//    @Test
//    public void wrong_attribute_name_compared_to_null() {
//        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
//                Query.of(Predicates.equal("name12312", null), mv),
//                Expected.of(QueryException.class));
//    }
//
//    @Test
//    public void primitiveNull_comparedToNull_matching() {
//        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
//                Query.of(Predicates.equal("name", null), mv),
//                Expected.of(HUNT_WITH_NULLS));
//    }
//
//    @Test
//    public void primitiveNull_comparedToNotNull_notMatching() {
//        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
//                Query.of(Predicates.equal("name", "Non-null-value"), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void nestedAttribute_firstIsNull_comparedToNotNull() {
//        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
//                Query.of(Predicates.equal("secondLimb.name", "Non-null-value"), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void nestedAttribute_firstIsNull_comparedToNull() {
//        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
//                Query.of(Predicates.equal("secondLimb.name", null), mv),
//                Expected.of(HUNT_WITH_NULLS));
//    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> parametrisationData() {
        return axes(
                asList(BINARY, OBJECT),
                asList(NO_INDEX, UNORDERED, ORDERED),
                asList(PORTABLE)
        );
    }

}
