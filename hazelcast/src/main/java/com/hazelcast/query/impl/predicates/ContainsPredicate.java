package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.collection.ArrayUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class ContainsPredicate extends AbstractPredicate implements Predicate, IndexAwarePredicate {

    private final Comparable value;

    public ContainsPredicate(String attribute, Comparable value) {
        super(attribute);
        this.value = value;
    }

    public boolean apply(Map.Entry mapEntry) {
        Object o = readAttribute(mapEntry);
        if (o == null) {
            return false;
        }
        if (o.getClass().isArray()) {
            return ArrayUtils.contains((Object[])o, value);
        }
        throw new IllegalArgumentException("Attribute " + attribute + " is type " + o.getClass());
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = getIndex(queryContext);
        return index.getRecords(value);
    }

}
