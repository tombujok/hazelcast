package com.hazelcast.query.extractor;

/**
 * Chaines the execution of the extractors taking the intermediary values
 */
public class ChainedExtractor extends ValueExtractor {

    public ChainedExtractor(ValueExtractor... extractor) {
    }

    @Override
    public Object extract(Object target) {
        return null;
    }
}
