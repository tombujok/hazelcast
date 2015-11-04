package com.hazelcast.query.impl.extraction.specification;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.extraction.AbstractExtractionTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.query.impl.extraction.AbstractExtractionSpecification.Index.NO_INDEX;
import static com.hazelcast.query.impl.extraction.AbstractExtractionSpecification.Index.ORDERED;
import static com.hazelcast.query.impl.extraction.AbstractExtractionSpecification.Index.UNORDERED;
import static com.hazelcast.query.impl.extraction.AbstractExtractionSpecification.Multivalue.ARRAY;
import static com.hazelcast.query.impl.extraction.AbstractExtractionSpecification.Multivalue.LIST;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.Finger;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.Person;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.finger;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.limb;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.person;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.tattoos;
import static java.util.Arrays.asList;

/**
 * Specification test that verifies the behavior of corner-cases extraction in arrays and collections.
 * <p/>
 * Extraction mechanism: IN-BUILT REFLECTION EXTRACTION
 * <p/>
 * This test is parametrised on two axes (see the parametrisationData() method):
 * - in memory format
 * - indexing
 * - extraction in collections and arrays
 */
@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExtractionInCollectionSpecTest extends AbstractExtractionTest {

    private static final Person BOND = person("Bond",
            limb("left-hand", tattoos(), finger("thumb"), finger(null)),
            limb("right-hand", tattoos("knife"), finger("middle"), finger("index"))
    );

    private static final Person KRUEGER = person("Krueger",
            limb("linke-hand", tattoos("bratwurst"), finger("Zeigefinger"), finger("Mittelfinger")),
            limb("rechte-hand", tattoos(), finger("Ringfinger"), finger("Daumen"))
    );

    private static final Person HUNT = person("Hunt",
            limb("left", null, new Finger[]{})
    );

    public ExtractionInCollectionSpecTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    @Test
    public void notComparable_returned() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[1].fingers_", "knife"), mv),
                Expected.of(IllegalArgumentException.class));
    }

    @Test
    public void indexOutOfBound() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[100].tattoos_[1]", "knife"), mv),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_negative() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[-1].tattoos_[1]", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_atLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[0].tattoos_[100]", "knife"), mv),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_negative_atLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[0].tattoos_[-1]", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[100].sdafasdf", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_atLeaf_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[0].tattoos_[100].asdfas", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void comparable_notPrimitive() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[0].fingers_[0]", finger("thumb")), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_notPrimitive_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[any].fingers_[0]", finger("thumb")), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[0].fingers_[0].name", "thumb"), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[any].fingers_[any].name", "thumb"), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[0].fingers_[0].name", null), mv),
                Expected.empty());
    }

    @Test
    public void comparable_notPrimitive_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[0].fingers_[0]", null), mv),
                Expected.empty());
    }

    @Test
    public void comparable_primitive_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[0].fingers_[1].name", null), mv),
                Expected.of(BOND));
    }

    @Test
    @Ignore
    // TODO @Jaromir BUG to fix -> null values handling in MultiResult like in IndexStore
    public void comparable_primitive_comparedToNull_reduced_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[0].fingers_[any].name", null), mv),
                Expected.of(BOND));
    }

    @Test
    @Ignore
    // TODO @Jaromir BUG to fix -> null values handling in MultiResult like in IndexStore
    public void comparable_primitive_reduced_atLeaf_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[any].fingers_[1].name", null), mv),
                Expected.of(BOND));
    }

    @Test
    @Ignore
    // TODO @Jaromir BUG to fix -> Collection throws NullPointer, Array throws QueryException
    // TODO Should be QueryException for consistency with null_arrayOrCollection_size
    public void null_fingersArrayOrCollection() {
        execute(Input.of(HUNT),
                Query.of(Predicates.equal("limbs_[0].fingers_[0].name", "index"), mv),
                Expected.of(NullPointerException.class));
    }

    @Test
    @Ignore
    // TODO @Jaromir BUG to fix -> see null_arrayOrCollection() test
    public void null_fingersArrayOrCollection_reduced() {
        execute(Input.of(HUNT),
                Query.of(Predicates.equal("limbs_[0].fingers_[any].name", "index"), mv),
                Expected.of(NullPointerException.class));
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> parametrisationData() {
        return axes(
                asList(BINARY, OBJECT),
                asList(NO_INDEX, UNORDERED, ORDERED),
                asList(ARRAY, LIST)
        );
    }

}
